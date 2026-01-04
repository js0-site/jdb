//! SSTable reader with PGM-index
//! 使用 PGM 索引的 SSTable 读取器

use std::{cmp::Ordering, path::Path};

use compio::{
  buf::{IntoInner, IoBuf},
  io::AsyncReadAtExt,
};
use crc32fast::Hasher;
use gxhash::gxhash64;
use jdb_base::Pos;
use jdb_fs::{FileLru, fs::open_read};
use jdb_pgm_index::PGMIndex;
use jdb_xorf::{BinaryFuse8, Filter};
use zerocopy::FromBytes;

use crate::{
  Error, Result, TableMeta,
  block::{DataBlock, last_key},
  footer::{FOOTER_SIZE, Footer},
  key_to_u64,
};

pub(crate) enum Pgm {
  None,
  Index {
    pgm: PGMIndex<u64>,
    // Max 8 bytes for u64 prefix
    // u64 前缀最多 8 字节
    prefix_len: u8,
  },
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

    // Read all meta blocks at once (Filter + Index + Offsets + PGM)
    // 一次性读取所有元数据块
    let meta_start = footer.filter_offset;
    let meta_len = (file_size - FOOTER_SIZE as u64 - meta_start) as usize;

    let meta_buf = {
      let buf = vec![0u8; meta_len];
      let slice = buf.slice(0..meta_len);
      let res = file.read_exact_at(slice, meta_start).await;
      res.0?;
      res.1.into_inner()
    };

    // Verify checksum
    // 验证校验和
    let checksum = {
      let mut h = Hasher::new();
      h.update(&meta_buf);
      h.finalize()
    };

    if checksum != footer.checksum {
      return Err(Error::ChecksumMismatch {
        expected: footer.checksum,
        actual: checksum,
      });
    }

    // Decode metadata sections
    // 解码元数据段
    let mut pos = 0;

    // Helper to read section
    // 读取段的辅助函数
    macro_rules! read_section {
      ($size:expr, $err:expr) => {{
        let size = $size as usize;
        // Bound check
        // 边界检查
        if pos + size > meta_buf.len() {
          return Err(Error::InvalidFooter);
        }
        let slice = &meta_buf[pos..pos + size];
        pos += size;
        bitcode::decode(slice).map_err(|_| $err)?
      }};
    }

    let filter: BinaryFuse8 = read_section!(footer.filter_size, Error::InvalidFilter);
    let first_keys: Vec<Box<[u8]>> = read_section!(footer.index_size, Error::InvalidIndex);
    let offsets: Vec<u64> = read_section!(footer.offsets_size, Error::InvalidOffsets);

    if offsets.len() != footer.block_count as usize {
      return Err(Error::InvalidOffsets);
    }

    // Read PGM
    // 读取 PGM 索引
    let pgm = if footer.pgm_size == 0 || footer.block_count <= 1 {
      Pgm::None
    } else {
      let size = footer.pgm_size as usize;
      if pos + size > meta_buf.len() {
        return Err(Error::InvalidFooter);
      }
      match bitcode::decode(&meta_buf[pos..pos + size]) {
        Ok(pgm) => Pgm::Index {
          pgm,
          prefix_len: footer.prefix_len,
        },
        Err(_) => Pgm::None,
      }
    };

    let data_end = footer.filter_offset;
    let block_count = footer.block_count as usize;

    // Build metadata
    // 构建元数据
    let mut meta = TableMeta::new(id);
    meta.file_size = file_size;

    if !first_keys.is_empty() {
      meta.min_key = first_keys.first().cloned().unwrap_or_default();

      if block_count > 0 {
        let last_idx = block_count - 1;
        let offset = offsets[last_idx];
        let size = Self::calc_block_size(&offsets, last_idx, data_end);
        let buf = vec![0u8; size];
        let slice = buf.slice(0..size);
        let res = file.read_exact_at(slice, offset).await;
        res.0?;

        meta.max_key = DataBlock::from_bytes(res.1.into_inner())
          .and_then(|b| last_key(&b))
          .or_else(|| first_keys.last().cloned())
          .unwrap_or_default();
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

  pub(crate) fn find_block(&self, key: &[u8]) -> usize {
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
      Pgm::None => bsearch(),
      Pgm::Index { pgm, prefix_len } => {
        let plen = *prefix_len as usize;
        let suffix = if key.len() > plen { &key[plen..] } else { &[] };

        let Some(approx) = pgm.get(key_to_u64(suffix)) else {
          return bsearch();
        };

        let lo = approx.saturating_sub(pgm.epsilon);
        let hi = (approx + pgm.epsilon + 2).min(n);

        // Linear search in the small range
        // 在小范围内线性搜索
        for i in lo..hi {
          // Logic: if key < next_block.first_key, then it must be in current block i
          // 逻辑：如果 key < next_block.first_key，则必须在当前块 i 中
          if i + 1 >= n || key < self.first_keys.get(i + 1).map_or(&[][..], |k| k.as_ref()) {
            return i;
          }
        }
        // Fallback to end of range
        // 回退到范围末尾
        (n - 1).max(lo)
      }
    }
  }

  #[inline]
  pub(crate) fn block_size(&self, idx: usize) -> usize {
    Self::calc_block_size(&self.offsets, idx, self.data_end)
  }

  pub(crate) async fn read_block(&self, idx: usize, file_lru: &mut FileLru) -> Result<DataBlock> {
    let offset = *self.offsets.get(idx).ok_or(Error::InvalidOffsets)?;
    let size = self.block_size(idx);
    let buf = vec![0u8; size];
    let buf = file_lru.read_into(self.meta.id, buf, offset).await?;
    DataBlock::from_bytes(buf).ok_or(Error::InvalidBlock { offset })
  }

  /// Get Pos by key
  /// 按键获取 Pos
  pub async fn get_pos(&self, key: &[u8], file_lru: &mut FileLru) -> Result<Option<Pos>> {
    // Check range first (pure memory, faster than bloom filter)
    // 先检查范围（纯内存操作，比布隆过滤器快）
    if !self.is_key_in_range(key) {
      return Ok(None);
    }

    if !self.may_contain(key) {
      return Ok(None);
    }

    let idx = self.find_block(key);
    if idx >= self.offsets.len() {
      return Ok(None);
    }

    let block = self.read_block(idx, file_lru).await?;
    let restart_idx = block.search_restart(key);

    for (k, pos) in block.iter_from_restart(restart_idx) {
      match k.as_slice().cmp(key) {
        Ordering::Equal => return Ok(Some(pos)),
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
}
