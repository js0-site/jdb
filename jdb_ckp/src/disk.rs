//! Disk format structures
//! 磁盘格式结构体

use jdb_base::{WalId, WalOffset};
use zerocopy::{Immutable, IntoBytes, little_endian::U64};

use crate::row::{
  KIND_ROTATE, KIND_SAVE, KIND_SST_ADD, KIND_SST_RM, MAGIC, MAGIC_SIZE, ROTATE_SIZE, SAVE_SIZE,
  SST_ADD_SIZE, SST_RM_SIZE,
};

pub(crate) trait ToDiskBytes<const N: usize>: IntoBytes + Immutable {
  const KIND: u8;

  fn to_array(&self) -> [u8; N] {
    let mut buf = [0u8; N];
    buf[0] = MAGIC;
    buf[1] = Self::KIND;
    let bytes = self.as_bytes();
    buf[2..2 + bytes.len()].copy_from_slice(bytes);
    // CRC covers kind + data (skip magic)
    // CRC 覆盖 kind + data（跳过 magic）
    let crc = crc32fast::hash(&buf[MAGIC_SIZE..N - 4]);
    buf[N - 4..].copy_from_slice(&crc.to_le_bytes());
    buf
  }
}

/// Disk format for save entry data
/// 保存条目的数据
#[derive(IntoBytes, Immutable)]
#[repr(C, packed)]
pub(crate) struct SaveWalPtr {
  wal_id: U64,
  offset: U64,
}

/// Disk format for rotate entry data
/// 轮转条目的数据
#[derive(IntoBytes, Immutable)]
#[repr(C, packed)]
pub(crate) struct Rotate {
  wal_id: U64,
}

/// Disk format for SST add entry
/// SST 添加条目的数据
#[derive(IntoBytes, Immutable)]
#[repr(C, packed)]
pub(crate) struct SstAdd {
  id: U64,
  level: u8,
}

/// Disk format for SST remove entry
/// SST 删除条目的数据
#[derive(IntoBytes, Immutable)]
#[repr(C, packed)]
pub(crate) struct SstRm {
  id: U64,
}

impl ToDiskBytes<SAVE_SIZE> for SaveWalPtr {
  const KIND: u8 = KIND_SAVE;
}

impl ToDiskBytes<ROTATE_SIZE> for Rotate {
  const KIND: u8 = KIND_ROTATE;
}

impl ToDiskBytes<SST_ADD_SIZE> for SstAdd {
  const KIND: u8 = KIND_SST_ADD;
}

impl ToDiskBytes<SST_RM_SIZE> for SstRm {
  const KIND: u8 = KIND_SST_RM;
}

impl SaveWalPtr {
  #[inline]
  pub(crate) fn new(wal_id: WalId, offset: WalOffset) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      offset: U64::new(offset),
    }
  }
}

impl Rotate {
  #[inline]
  pub(crate) fn new(wal_id: WalId) -> Self {
    Self {
      wal_id: U64::new(wal_id),
    }
  }
}

impl SstAdd {
  #[inline]
  pub(crate) fn new(id: u64, level: u8) -> Self {
    Self {
      id: U64::new(id),
      level,
    }
  }
}

impl SstRm {
  #[inline]
  pub(crate) fn new(id: u64) -> Self {
    Self { id: U64::new(id) }
  }
}
