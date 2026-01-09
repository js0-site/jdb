//! SSTable reader with PGM-index
//! 使用 PGM 索引的 SSTable 读取器

use std::path::Path;

use compio::{
  buf::{IntoInner, IoBuf},
  io::AsyncReadAtExt,
};
use crc32fast::Hasher;
use gxhash::gxhash64;
use jdb_base::Pos;
use jdb_fs::{FileLru, fs::open_read};
use jdb_pgm::Pgm;
use jdb_sst::{
  Error, FOOT_SIZE, Foot, Meta, Result,
  block::{Block, last_key},
};
use jdb_xorf::{BinaryFuse8, Filter};
use zerocopy::FromBytes;

pub(crate) enum PgmIndex {
  None,
  Index {
    pgm: Pgm<u64>,
    // Max 8 bytes for u64 prefix
    // u64 前缀最多 8 字节
    prefix_len: u8,
  },
}

/// SSTable info with PGM-index
/// 使用 PGM 索引的 SSTable 信息
pub struct Table {
  filter: BinaryFuse8,
  offsets: Vec<u64>,
  pgm: PgmIndex,
  first_keys: Vec<Box<[u8]>>,
  meta: Meta,
  data_end: u64,
}

impl Table {
  pub async fn load(path: &Path, id: u64) -> Result<Self> {
    let file = open_read(path).await?;
    let file_size = file.metadata().await?.len();

    if file_size < FOOT_SIZE as u64 {
      return Err(Error::SstTooSmall {
        size: file_size as usize,
      });
    }

    // Read foot
    // 读取尾部
    let foot = {
      let buf = vec![0u8; FOOT_SIZE];
      let slice = buf.slice(0..FOOT_SIZE);
      let res = file
        .read_exact_at(slice, file_size - FOOT_SIZE as u64)
        .await;
      res.0?;
      Foot::read_from_bytes(&res.1.into_inner()).map_err(|_| Error::InvalidFoot)?
    };

    // Read all meta blocks at once (Filter + Index + Offsets + PGM)
    // 一次性读取所有元数据块
    let meta_start = foot.filter_offset;
    let meta_len = (file_size - FOOT_SIZE as u64 - meta_start) as usize;

    let meta_buf = {
      let buf = vec![0u8; meta_len];
      let slice = buf.slice(0..meta_len);
      let res = file.read_exact_at(slice, meta_start).await;
      res.0?;
      res.1.into_inner()
    };

    // Verify checksum (includes version)
    // 验证校验和（包含 version）
    let checksum = {
      let mut h = Hasher::new();
      h.update(&meta_buf);
      h.update(&[foot.version]);
      h.finalize()
    };

    if checksum != foot.checksum {
      return Err(Error::ChecksumMismatch {
        expected: foot.checksum,
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
        if pos + size > meta_buf.len() {
          return Err(Error::InvalidFoot);
        }
        let slice = &meta_buf[pos..pos + size];
        pos += size;
        bitcode::decode(slice).map_err(|_| $err)?
      }};
    }

    let filter: BinaryFuse8 = read_section!(foot.filter_size, Error::InvalidFilter);
    let first_keys: Vec<Box<[u8]>> = read_section!(foot.index_size, Error::InvalidIndex);
    let offsets: Vec<u64> = read_section!(foot.offsets_size, Error::InvalidOffsets);

    if offsets.len() != foot.block_count as usize {
      return Err(Error::InvalidOffsets);
    }

    // Read PGM
    // 读取 PGM 索引
    let pgm = if foot.pgm_size == 0 || foot.block_count <= 1 {
      PgmIndex::None
    } else {
      let size = foot.pgm_size as usize;
      if pos + size > meta_buf.len() {
        return Err(Error::InvalidFoot);
      }
      match bitcode::decode(&meta_buf[pos..pos + size]) {
        Ok(pgm) => PgmIndex::Index {
          pgm,
          prefix_len: foot.prefix_len,
        },
        Err(_) => PgmIndex::None,
      }
    };

    let data_end = foot.filter_offset;
    let block_count = foot.block_count as usize;

    // Build metadata
    // 构建元数据
    let mut meta = Meta::new(id);
    meta.file_size = file_size;

    if let Some(first) = first_keys.first() {
      meta.min_key = first.clone();

      if block_count > 0 {
        let last_idx = block_count - 1;
        let offset = offsets[last_idx];
        let size = Self::calc_block_size(&offsets, last_idx, data_end);
        let buf = vec![0u8; size];
        let slice = buf.slice(0..size);
        let res = file.read_exact_at(slice, offset).await;
        res.0?;

        meta.max_key = Block::from_bytes(res.1.into_inner())
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
    self.meta.contains(key)
  }

  pub fn find_block(&self, key: &[u8]) -> usize {
    let n = self.offsets.len();
    if n <= 1 {
      return 0;
    }

    match &self.pgm {
      PgmIndex::None => self
        .first_keys
        .partition_point(|k| k.as_ref() <= key)
        .saturating_sub(1),
      PgmIndex::Index { pgm, prefix_len } => {
        let plen = *prefix_len as usize;
        let suffix: &[u8] = if key.len() >= plen { &key[plen..] } else { &[] };

        let idx = pgm.find(suffix, |i| {
          self.first_keys.get(i).map(|fk| {
            if fk.len() >= plen {
              &fk[plen..]
            } else {
              &[][..]
            }
          })
        });

        idx.saturating_sub(1).min(n - 1)
      }
    }
  }

  #[inline]
  pub fn block_size(&self, idx: usize) -> usize {
    Self::calc_block_size(&self.offsets, idx, self.data_end)
  }

  pub async fn read_block(&self, idx: usize, file_lru: &mut FileLru) -> Result<Block> {
    let offset = *self.offsets.get(idx).ok_or(Error::InvalidOffsets)?;
    let size = self.block_size(idx);
    let buf = vec![0u8; size];
    let buf = file_lru.read_into(self.meta.id, buf, offset).await?;
    Block::from_bytes(buf).ok_or(Error::InvalidBlock { offset })
  }

  /// Get Pos by key (with range and bloom filter check)
  /// 按键获取 Pos（带范围和布隆过滤器检查）
  pub async fn get_pos(&self, key: &[u8], file_lru: &mut FileLru) -> Result<Option<Pos>> {
    if !self.is_key_in_range(key) || !self.may_contain(key) {
      return Ok(None);
    }
    self.get_pos_unchecked(key, file_lru).await
  }

  /// Get Pos by key (skip range/bloom check, caller must verify)
  /// 按键获取 Pos（跳过范围/布隆检查，调用者需验证）
  pub async fn get_pos_unchecked(&self, key: &[u8], file_lru: &mut FileLru) -> Result<Option<Pos>> {
    let idx = self.find_block(key);
    if idx >= self.offsets.len() {
      return Ok(None);
    }

    let block = self.read_block(idx, file_lru).await?;

    let prefix_len = block.prefix.len();
    if key.len() < prefix_len || key[..prefix_len] != *block.prefix {
      return Ok(None);
    }
    let target_suffix = &key[prefix_len..];

    let restart_idx = block.search_restart(key);
    let mut buf = Vec::with_capacity(64);

    Ok(block.find_key(target_suffix, restart_idx, &mut buf))
  }

  #[inline]
  pub fn meta(&self) -> &Meta {
    &self.meta
  }

  #[inline]
  pub fn block_count(&self) -> usize {
    self.offsets.len()
  }
}
