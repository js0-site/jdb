//! SSTable reader with lazy file handle
//! SSTable 懒加载文件句柄读取器

use std::path::PathBuf;

use autoscale_cuckoo_filter::CuckooFilter;
use compio::buf::{IoBuf, IntoInner};
use compio::io::AsyncReadAtExt;
use crc32fast::Hasher;
use jdb_base::{FileLru, open_read};
use zerocopy::FromBytes;

use super::{FOOTER_SIZE, Footer, TableMeta};
use crate::{DataBlock, Entry, Result};

/// Index entry for block lookup
/// 块查找的索引条目
#[derive(Debug, Clone)]
struct IndexEntry {
  last_key: Box<[u8]>,
  offset: u64,
  size: u32,
}

/// SSTable info (metadata only, no file handle)
/// SSTable 信息（仅元数据，无文件句柄）
pub struct TableInfo {
  filter: CuckooFilter<[u8]>,
  index: Vec<IndexEntry>,
  meta: TableMeta,
}

impl TableInfo {
  /// Load SSTable info from file
  /// 从文件加载 SSTable 信息
  pub async fn load(path: &PathBuf, id: u64) -> Result<Self> {
    let file = open_read(path).await?;

    let file_meta = file.metadata().await?;
    let file_size = file_meta.len();

    if file_size < FOOTER_SIZE as u64 {
      return Err(crate::Error::Corruption {
        msg: format!("SSTable too small: {file_size} bytes"),
      });
    }

    // Read footer
    // 读取尾部
    let footer_offset = file_size - FOOTER_SIZE as u64;
    let buf = vec![0u8; FOOTER_SIZE];
    let slice = buf.slice(0..FOOTER_SIZE);
    let res = file.read_exact_at(slice, footer_offset).await;
    res.0?;
    let buf = res.1.into_inner();

    let footer = Footer::read_from_bytes(&buf).map_err(|_| crate::Error::Corruption {
      msg: "Invalid footer".into(),
    })?;

    // Verify checksum
    // 验证校验和
    let data_size = footer.filter_offset() + footer.filter_size() + footer.index_size();
    let mut hasher = Hasher::new();

    let buf = vec![0u8; data_size as usize];
    let slice = buf.slice(0..data_size as usize);
    let res = file.read_exact_at(slice, 0).await;
    res.0?;
    let buf = res.1.into_inner();
    hasher.update(&buf);

    let computed_checksum = hasher.finalize();
    if computed_checksum != footer.checksum() {
      return Err(crate::Error::Corruption {
        msg: format!(
          "Checksum mismatch: expected {}, got {computed_checksum}",
          footer.checksum()
        ),
      });
    }

    // Read filter block
    // 读取过滤器块
    let filter_size = footer.filter_size() as usize;
    let buf = vec![0u8; filter_size];
    let slice = buf.slice(0..filter_size);
    let res = file.read_exact_at(slice, footer.filter_offset()).await;
    res.0?;
    let buf = res.1.into_inner();

    let filter: CuckooFilter<[u8]> =
      bitcode::decode(&buf).map_err(|e| crate::Error::Corruption {
        msg: format!("Invalid filter: {e}"),
      })?;

    // Read index block
    // 读取索引块
    let index_size = footer.index_size() as usize;
    let buf = vec![0u8; index_size];
    let slice = buf.slice(0..index_size);
    let res = file.read_exact_at(slice, footer.index_offset()).await;
    res.0?;
    let buf = res.1.into_inner();

    let index = Self::decode_index(&buf)?;

    // Build metadata
    // 构建元数据
    let mut meta = TableMeta::new(id);
    meta.file_size = file_size;
    meta.item_count = index.len() as u64;

    if let Some(last) = index.last() {
      meta.max_key = last.last_key.clone();
    }

    // Read first block to get min_key
    // 读取第一个块以获取 min_key
    if let Some(first) = index.first() {
      let buf = vec![0u8; first.size as usize];
      let slice = buf.slice(0..first.size as usize);
      let res = file.read_exact_at(slice, first.offset).await;
      res.0?;
      let buf = res.1.into_inner();

      if let Some(block) = DataBlock::from_bytes(buf) {
        if let Some((key, _)) = block.iter().next() {
          meta.min_key = key.into_boxed_slice();
        }
      }
    }

    Ok(Self {
      filter,
      index,
      meta,
    })
  }

  /// Decode index block
  /// 解码索引块
  fn decode_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
    if data.len() < 4 {
      return Err(crate::Error::Corruption {
        msg: "Index too small".into(),
      });
    }

    let entry_count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = 4;

    for _ in 0..entry_count {
      if pos + 2 > data.len() {
        return Err(crate::Error::Corruption {
          msg: "Index truncated".into(),
        });
      }

      let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
      pos += 2;

      if pos + key_len + 12 > data.len() {
        return Err(crate::Error::Corruption {
          msg: "Index entry truncated".into(),
        });
      }

      let last_key = data[pos..pos + key_len].into();
      pos += key_len;

      let offset = u64::from_le_bytes([
        data[pos],
        data[pos + 1],
        data[pos + 2],
        data[pos + 3],
        data[pos + 4],
        data[pos + 5],
        data[pos + 6],
        data[pos + 7],
      ]);
      pos += 8;

      let size = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
      pos += 4;

      entries.push(IndexEntry {
        last_key,
        offset,
        size,
      });
    }

    Ok(entries)
  }

  /// Check if key may exist (cuckoo filter)
  /// 检查键是否可能存在（布谷鸟过滤器）
  #[inline]
  pub fn may_contain(&self, key: &[u8]) -> bool {
    self.filter.contains(key)
  }

  /// Check if key is in range [min_key, max_key]
  /// 检查键是否在范围内
  #[inline]
  pub fn is_key_in_range(&self, key: &[u8]) -> bool {
    self.meta.contains_key(key)
  }

  /// Get entry by key using FileLru
  /// 通过 FileLru 按键获取条目
  pub async fn get(&self, key: &[u8], files: &mut FileLru) -> Result<Option<Entry>> {
    if !self.may_contain(key) {
      return Ok(None);
    }

    let block_idx = self.find_block(key);
    if block_idx >= self.index.len() {
      return Ok(None);
    }

    let block = self.read_block(block_idx, files).await?;
    for (k, entry) in block.iter() {
      match k.as_slice().cmp(key) {
        std::cmp::Ordering::Equal => return Ok(Some(entry)),
        std::cmp::Ordering::Greater => return Ok(None),
        std::cmp::Ordering::Less => continue,
      }
    }

    Ok(None)
  }

  /// Find block index for key
  /// 查找键所在的块索引
  fn find_block(&self, key: &[u8]) -> usize {
    self
      .index
      .partition_point(|entry| entry.last_key.as_ref() < key)
  }

  /// Read block at index using FileLru
  /// 通过 FileLru 读取指定索引的块
  async fn read_block(&self, idx: usize, files: &mut FileLru) -> Result<DataBlock> {
    let entry = &self.index[idx];
    let buf = vec![0u8; entry.size as usize];
    let (res, buf) = files.read_into(self.meta.id, buf, entry.offset).await;
    res?;

    DataBlock::from_bytes(buf).ok_or_else(|| crate::Error::Corruption {
      msg: format!("Invalid block at offset {}", entry.offset),
    })
  }

  /// Get metadata
  /// 获取元数据
  #[inline]
  pub fn meta(&self) -> &TableMeta {
    &self.meta
  }

  /// Get block count
  /// 获取块数量
  #[inline]
  pub fn block_count(&self) -> usize {
    self.index.len()
  }

  /// Load all blocks and create iterator
  /// 加载所有块并创建迭代器
  pub async fn iter(&self, files: &mut FileLru) -> Result<SSTableIter> {
    let mut entries = Vec::new();

    for idx in 0..self.index.len() {
      let block = self.read_block(idx, files).await?;
      for (key, entry) in block.iter() {
        if !entry.is_tombstone() {
          entries.push((key.into_boxed_slice(), entry));
        }
      }
    }

    Ok(SSTableIter {
      entries,
      lo: 0,
      hi_offset: 0,
    })
  }

  /// Load all blocks and create iterator (including tombstones)
  /// 加载所有块并创建迭代器（包含删除标记）
  pub async fn iter_with_tombstones(&self, files: &mut FileLru) -> Result<SSTableIterWithTombstones> {
    let mut entries = Vec::new();

    for idx in 0..self.index.len() {
      let block = self.read_block(idx, files).await?;
      for (key, entry) in block.iter() {
        entries.push((key.into_boxed_slice(), entry));
      }
    }

    Ok(SSTableIterWithTombstones {
      entries,
      lo: 0,
      hi_offset: 0,
    })
  }

  /// Create range iterator
  /// 创建范围迭代器
  pub async fn range(
    &self,
    start: &[u8],
    end: &[u8],
    files: &mut FileLru,
  ) -> Result<SSTableIter> {
    let mut entries = Vec::new();

    let start_block = self.find_block(start);
    let end_block = self.find_block(end);

    for idx in start_block..=end_block.min(self.index.len().saturating_sub(1)) {
      if idx >= self.index.len() {
        break;
      }
      let block = self.read_block(idx, files).await?;
      for (key, entry) in block.iter() {
        if key.as_slice() >= start && key.as_slice() <= end && !entry.is_tombstone() {
          entries.push((key.into_boxed_slice(), entry));
        }
      }
    }

    Ok(SSTableIter {
      entries,
      lo: 0,
      hi_offset: 0,
    })
  }
}

/// SSTable iterator (skips tombstones)
/// SSTable 迭代器（跳过删除标记）
pub struct SSTableIter {
  entries: Vec<(Box<[u8]>, Entry)>,
  lo: usize,
  hi_offset: usize,
}

impl Iterator for SSTableIter {
  type Item = (Box<[u8]>, Entry);

  fn next(&mut self) -> Option<Self::Item> {
    let remaining = self.entries.len().saturating_sub(self.hi_offset);
    if self.lo >= remaining {
      return None;
    }
    let item = self.entries[self.lo].clone();
    self.lo += 1;
    Some(item)
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    let remaining = self.entries.len().saturating_sub(self.lo + self.hi_offset);
    (remaining, Some(remaining))
  }
}

impl DoubleEndedIterator for SSTableIter {
  fn next_back(&mut self) -> Option<Self::Item> {
    let remaining = self.entries.len().saturating_sub(self.hi_offset);
    if self.lo >= remaining {
      return None;
    }
    self.hi_offset += 1;
    let idx = self.entries.len() - self.hi_offset;
    Some(self.entries[idx].clone())
  }
}

impl ExactSizeIterator for SSTableIter {}

/// SSTable iterator with tombstones
/// 包含删除标记的 SSTable 迭代器
pub struct SSTableIterWithTombstones {
  entries: Vec<(Box<[u8]>, Entry)>,
  lo: usize,
  hi_offset: usize,
}

impl Iterator for SSTableIterWithTombstones {
  type Item = (Box<[u8]>, Entry);

  fn next(&mut self) -> Option<Self::Item> {
    let remaining = self.entries.len().saturating_sub(self.hi_offset);
    if self.lo >= remaining {
      return None;
    }
    let item = self.entries[self.lo].clone();
    self.lo += 1;
    Some(item)
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    let remaining = self.entries.len().saturating_sub(self.lo + self.hi_offset);
    (remaining, Some(remaining))
  }
}

impl DoubleEndedIterator for SSTableIterWithTombstones {
  fn next_back(&mut self) -> Option<Self::Item> {
    let remaining = self.entries.len().saturating_sub(self.hi_offset);
    if self.lo >= remaining {
      return None;
    }
    self.hi_offset += 1;
    let idx = self.entries.len() - self.hi_offset;
    Some(self.entries[idx].clone())
  }
}

impl ExactSizeIterator for SSTableIterWithTombstones {}
