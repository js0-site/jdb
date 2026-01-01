//! SSTable reader
//! SSTable 读取器
//!
//! Reads SSTable files with filter and index support.
//! 读取带过滤器和索引支持的 SSTable 文件。

use std::path::PathBuf;

use autoscale_cuckoo_filter::CuckooFilter;
use compio::{
  buf::{IntoInner, IoBuf},
  fs::File,
  io::AsyncReadAtExt,
};
use crc32fast::Hasher;
use zerocopy::FromBytes;

use crate::{DataBlock, Entry, Result};

use super::{Footer, TableMeta, FOOTER_SIZE};

/// Index entry for block lookup
/// 块查找的索引条目
#[derive(Debug, Clone)]
struct IndexEntry {
  last_key: Box<[u8]>,
  offset: u64,
  size: u32,
}

/// SSTable reader
/// SSTable 读取器
pub struct Reader {
  path: PathBuf,
  file: File,
  filter: CuckooFilter<[u8]>,
  index: Vec<IndexEntry>,
  meta: TableMeta,
  footer: Footer,
}

impl Reader {
  /// Open SSTable file
  /// 打开 SSTable 文件
  pub async fn open(path: PathBuf, id: u64) -> Result<Self> {
    let file = compio::fs::OpenOptions::new()
      .read(true)
      .open(&path)
      .await?;

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

    if !footer.is_valid() {
      return Err(crate::Error::Corruption {
        msg: "Invalid footer magic".into(),
      });
    }

    // Verify checksum
    // 验证校验和
    let data_size = footer.filter_offset() + footer.filter_size() + footer.index_size();
    let mut hasher = Hasher::new();

    // Read and hash all data before footer
    // 读取并哈希尾部之前的所有数据
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
          "Checksum mismatch: expected {}, got {}",
          footer.checksum(),
          computed_checksum
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
    meta.item_count = index.len() as u64; // Approximate, actual count in blocks
                                          // 近似值，实际数量在块中

    // max_key is the last key in the last block
    // max_key 是最后一个块的最后一个键
    if let Some(last) = index.last() {
      meta.max_key = last.last_key.clone();
    }

    let mut reader = Self {
      path,
      file,
      filter,
      index,
      meta,
      footer,
    };

    // Read first block to get min_key
    // 读取第一个块以获取 min_key
    if !reader.index.is_empty() {
      let first_block = reader.read_block(0).await?;
      if let Some((key, _)) = first_block.iter().next() {
        reader.meta.min_key = key.into_boxed_slice();
      }
    }

    Ok(reader)
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

  /// Get entry by key
  /// 通过键获取条目
  pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
    // Check filter first
    // 先检查过滤器
    if !self.may_contain(key) {
      return Ok(None);
    }

    // Binary search for block
    // 二分查找块
    let block_idx = self.find_block(key);
    if block_idx >= self.index.len() {
      return Ok(None);
    }

    // Read and search block
    // 读取并搜索块
    let block = self.read_block(block_idx).await?;
    for (k, entry) in block.iter() {
      match k.as_slice().cmp(key) {
        std::cmp::Ordering::Equal => return Ok(Some(entry)),
        std::cmp::Ordering::Greater => return Ok(None),
        std::cmp::Ordering::Less => continue,
      }
    }

    Ok(None)
  }

  /// Find block index for key using binary search
  /// 使用二分查找找到键所在的块索引
  fn find_block(&self, key: &[u8]) -> usize {
    // Find first block where last_key >= key
    // 找到第一个 last_key >= key 的块
    self
      .index
      .partition_point(|entry| entry.last_key.as_ref() < key)
  }

  /// Read block at index
  /// 读取指定索引的块
  async fn read_block(&self, idx: usize) -> Result<DataBlock> {
    let entry = &self.index[idx];
    let buf = vec![0u8; entry.size as usize];
    let slice = buf.slice(0..entry.size as usize);
    let res = self.file.read_exact_at(slice, entry.offset).await;
    res.0?;
    let buf = res.1.into_inner();

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

  /// Get file path
  /// 获取文件路径
  #[inline]
  pub fn path(&self) -> &PathBuf {
    &self.path
  }

  /// Get footer
  /// 获取尾部
  #[inline]
  pub fn footer(&self) -> &Footer {
    &self.footer
  }

  /// Load all blocks and create iterator
  /// 加载所有块并创建迭代器
  ///
  /// Returns iterator that skips tombstones.
  /// 返回跳过删除标记的迭代器。
  pub async fn iter(&self) -> Result<SSTableIter> {
    let mut entries = Vec::new();

    for idx in 0..self.index.len() {
      let block = self.read_block(idx).await?;
      for (key, entry) in block.iter() {
        // Skip tombstones
        // 跳过删除标记
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
  pub async fn iter_with_tombstones(&self) -> Result<SSTableIterWithTombstones> {
    let mut entries = Vec::new();

    for idx in 0..self.index.len() {
      let block = self.read_block(idx).await?;
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
  pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<SSTableIter> {
    let mut entries = Vec::new();

    // Find starting block
    // 找到起始块
    let start_block = self.find_block(start);
    let end_block = self.find_block(end);

    for idx in start_block..=end_block.min(self.index.len().saturating_sub(1)) {
      if idx >= self.index.len() {
        break;
      }
      let block = self.read_block(idx).await?;
      for (key, entry) in block.iter() {
        if key.as_slice() >= start && key.as_slice() <= end {
          // Skip tombstones
          // 跳过删除标记
          if !entry.is_tombstone() {
            entries.push((key.into_boxed_slice(), entry));
          }
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
