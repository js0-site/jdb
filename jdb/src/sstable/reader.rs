//! SSTable reader with PGM-index
//! 使用 PGM 索引的 SSTable 读取器

use std::{cmp::Ordering, path::Path};

use compio::{
  buf::{IntoInner, IoBuf},
  io::AsyncReadAtExt,
};
use crc32fast::Hasher;
use gxhash::gxhash64;
use jdb_fs::{FileLru, open_read};
use jdb_pgm_index::PGMIndex;
use jdb_xorf::{BinaryFuse8, Filter};
use zerocopy::FromBytes;

use super::{FOOTER_SIZE, Footer, TableMeta, key_to_u64};
use crate::{DataBlock, Entry, Error, Result};

/// PGM index wrapper
/// PGM 索引包装器
enum Pgm {
  Single,
  Multi(PGMIndex<u64>),
}

/// SSTable info with PGM-index
/// 使用 PGM 索引的 SSTable 信息
pub struct TableInfo {
  filter: BinaryFuse8,
  offsets: Vec<u64>,
  pgm: Pgm,
  first_keys: Vec<Box<[u8]>>,
  meta: TableMeta,
  data_end: u64,
}

impl TableInfo {
  pub async fn load(path: &Path, id: u64) -> Result<Self> {
    let file = open_read(path).await?;
    let file_size = file.metadata().await?.len();

    if file_size < FOOTER_SIZE as u64 {
      return Err(Error::SstTooSmall { size: file_size });
    }

    // Read footer
    // 读取尾部
    let footer = {
      let buf = vec![0u8; FOOTER_SIZE];
      let slice = buf.slice(0..FOOTER_SIZE);
      let res = file
        .read_exact_at(slice, file_size - FOOTER_SIZE as u64)
        .await;
      res.0?;
      Footer::read_from_bytes(&res.1.into_inner()).map_err(|_| Error::InvalidFooter)?
    };

    // Verify checksum
    // 验证校验和
    let data_len = (footer.filter_offset()
      + footer.filter_size() as u64
      + footer.offsets_size() as u64
      + footer.pgm_size() as u64) as usize;

    let checksum = {
      let buf = vec![0u8; data_len];
      let slice = buf.slice(0..data_len);
      let res = file.read_exact_at(slice, 0).await;
      res.0?;
      let mut h = Hasher::new();
      h.update(&res.1.into_inner());
      h.finalize()
    };

    if checksum != footer.checksum() {
      return Err(Error::ChecksumMismatch {
        expected: footer.checksum(),
        actual: checksum,
      });
    }

    // Read filter
    // 读取过滤器
    let filter = {
      let size = footer.filter_size() as usize;
      let buf = vec![0u8; size];
      let slice = buf.slice(0..size);
      let res = file.read_exact_at(slice, footer.filter_offset()).await;
      res.0?;
      bitcode::decode(&res.1.into_inner()).map_err(|_| Error::InvalidFilter)?
    };

    // Read offsets
    // 读取偏移数组
    let offsets: Vec<u64> = {
      let size = footer.offsets_size() as usize;
      let buf = vec![0u8; size];
      let slice = buf.slice(0..size);
      let res = file.read_exact_at(slice, footer.offsets_offset()).await;
      res.0?;
      bitcode::decode(&res.1.into_inner()).map_err(|_| Error::InvalidOffsets)?
    };

    // Read PGM
    // 读取 PGM 索引
    let pgm = if footer.pgm_size() == 0 || footer.block_count() <= 1 {
      Pgm::Single
    } else {
      let size = footer.pgm_size() as usize;
      let buf = vec![0u8; size];
      let slice = buf.slice(0..size);
      let res = file.read_exact_at(slice, footer.pgm_offset()).await;
      res.0?;
      bitcode::decode(&res.1.into_inner())
        .map(Pgm::Multi)
        .unwrap_or(Pgm::Single)
    };

    let data_end = footer.filter_offset();
    let block_count = footer.block_count() as usize;

    // Load first keys and build metadata
    // 加载首键并构建元数据
    let mut first_keys = Vec::with_capacity(block_count);
    let mut meta = TableMeta::new(id);
    meta.file_size = file_size;

    for i in 0..block_count {
      let offset = offsets[i];
      let size = Self::calc_block_size(&offsets, i, data_end);

      let buf = vec![0u8; size];
      let slice = buf.slice(0..size);
      let res = file.read_exact_at(slice, offset).await;
      res.0?;

      if let Some(block) = DataBlock::from_bytes(res.1.into_inner()) {
        let mut iter = block.iter();
        if let Some((key, _)) = iter.next() {
          if i == 0 {
            meta.min_key = key.clone().into_boxed_slice();
          }
          first_keys.push(key.into_boxed_slice());

          if i == block_count - 1 {
            if let Some((last_key, _)) = iter.next_back() {
              meta.max_key = last_key.into_boxed_slice();
            } else {
              meta.max_key = first_keys[i].clone();
            }
          }
        } else {
          first_keys.push(Box::new([]));
        }
      } else {
        first_keys.push(Box::new([]));
      }
    }

    Ok(Self {
      filter,
      offsets,
      pgm,
      first_keys,
      meta,
      data_end,
    })
  }

  #[inline]
  fn calc_block_size(offsets: &[u64], idx: usize, data_end: u64) -> usize {
    if idx + 1 < offsets.len() {
      (offsets[idx + 1] - offsets[idx]) as usize
    } else {
      (data_end - offsets[idx]) as usize
    }
  }

  #[inline]
  pub fn may_contain(&self, key: &[u8]) -> bool {
    self.filter.contains(&gxhash64(key, 0))
  }

  #[inline]
  pub fn is_key_in_range(&self, key: &[u8]) -> bool {
    self.meta.contains_key(key)
  }

  fn find_block(&self, key: &[u8]) -> usize {
    let n = self.offsets.len();
    if n <= 1 {
      return 0;
    }

    let bsearch = || {
      self
        .first_keys
        .partition_point(|k| k.as_ref() <= key)
        .saturating_sub(1)
    };

    match &self.pgm {
      Pgm::Single => bsearch(),
      Pgm::Multi(pgm) => {
        let Some(approx) = pgm.get(key_to_u64(key)) else {
          return bsearch();
        };

        let lo = approx.saturating_sub(pgm.epsilon);
        let hi = (approx + pgm.epsilon + 1).min(n);

        for i in lo..hi {
          if i + 1 < n && key < self.first_keys[i + 1].as_ref() {
            return i;
          }
        }
        hi.saturating_sub(1)
      }
    }
  }

  #[inline]
  fn block_size(&self, idx: usize) -> usize {
    Self::calc_block_size(&self.offsets, idx, self.data_end)
  }

  async fn read_block(&self, idx: usize, files: &mut FileLru) -> Result<DataBlock> {
    let offset = self.offsets[idx];
    let buf = vec![0u8; self.block_size(idx)];
    let (res, buf) = files.read_into(self.meta.id, buf, offset).await;
    res?;
    DataBlock::from_bytes(buf).ok_or(Error::InvalidBlock { offset })
  }

  pub async fn get(&self, key: &[u8], files: &mut FileLru) -> Result<Option<Entry>> {
    if !self.may_contain(key) {
      return Ok(None);
    }

    let idx = self.find_block(key);
    if idx >= self.offsets.len() {
      return Ok(None);
    }

    let block = self.read_block(idx, files).await?;
    for (k, entry) in block.iter() {
      match k.as_slice().cmp(key) {
        Ordering::Equal => return Ok(Some(entry)),
        Ordering::Greater => return Ok(None),
        Ordering::Less => {}
      }
    }
    Ok(None)
  }

  #[inline]
  pub fn meta(&self) -> &TableMeta {
    &self.meta
  }

  #[inline]
  pub fn block_count(&self) -> usize {
    self.offsets.len()
  }

  async fn collect(&self, files: &mut FileLru, with_tomb: bool) -> Result<Vec<(Box<[u8]>, Entry)>> {
    let mut out = Vec::new();
    for idx in 0..self.offsets.len() {
      let block = self.read_block(idx, files).await?;
      for (key, entry) in block.iter() {
        if with_tomb || !entry.is_tombstone() {
          out.push((key.into_boxed_slice(), entry));
        }
      }
    }
    Ok(out)
  }

  pub async fn iter(&self, files: &mut FileLru) -> Result<SSTableIter> {
    Ok(SSTableIter::new(self.collect(files, false).await?))
  }

  pub async fn iter_with_tombstones(&self, files: &mut FileLru) -> Result<SSTableIter> {
    Ok(SSTableIter::new(self.collect(files, true).await?))
  }

  pub async fn range(&self, lo: &[u8], hi: &[u8], files: &mut FileLru) -> Result<SSTableIter> {
    let mut out = Vec::new();
    let start = self.find_block(lo);
    let end = self
      .find_block(hi)
      .min(self.offsets.len().saturating_sub(1));

    for idx in start..=end {
      let block = self.read_block(idx, files).await?;
      for (key, entry) in block.iter() {
        let k = key.as_slice();
        if k >= lo && k <= hi && !entry.is_tombstone() {
          out.push((key.into_boxed_slice(), entry));
        }
      }
    }
    Ok(SSTableIter::new(out))
  }
}

/// SSTable iterator
/// SSTable 迭代器
pub struct SSTableIter {
  entries: Vec<(Box<[u8]>, Entry)>,
  lo: usize,
  hi: usize,
}

impl SSTableIter {
  #[inline]
  fn new(entries: Vec<(Box<[u8]>, Entry)>) -> Self {
    let hi = entries.len();
    Self { entries, lo: 0, hi }
  }
}

impl Iterator for SSTableIter {
  type Item = (Box<[u8]>, Entry);

  fn next(&mut self) -> Option<Self::Item> {
    if self.lo >= self.hi {
      return None;
    }
    let item = self.entries[self.lo].clone();
    self.lo += 1;
    Some(item)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    let r = self.hi.saturating_sub(self.lo);
    (r, Some(r))
  }
}

impl DoubleEndedIterator for SSTableIter {
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.lo >= self.hi {
      return None;
    }
    self.hi -= 1;
    Some(self.entries[self.hi].clone())
  }
}

impl ExactSizeIterator for SSTableIter {}
