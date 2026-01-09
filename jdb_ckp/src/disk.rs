//! Disk format structures
//! 磁盘格式结构体

use jdb_base::{WalId, WalOffset};
use jdb_fs::kv::{CRC_SIZE, HEAD_SIZE, to_disk};
use zerocopy::{Immutable, IntoBytes, little_endian::U64};

use crate::row::{KIND_ROTATE, KIND_SAVE, KIND_SST_ADD, KIND_SST_RM};

/// Rotate entry size
/// 轮转条目大小
pub const ROTATE_SIZE: usize = HEAD_SIZE + size_of::<Rotate>() + CRC_SIZE;

/// Save entry size
/// 保存条目大小
pub const SAVE_SIZE: usize = HEAD_SIZE + size_of::<SaveWalPtr>() + CRC_SIZE;

/// SstAdd entry size
/// SST 添加条目大小
pub const SST_ADD_SIZE: usize = HEAD_SIZE + size_of::<SstAdd>() + CRC_SIZE;

/// SstRm entry size
/// SST 删除条目大小
pub const SST_RM_SIZE: usize = HEAD_SIZE + size_of::<SstRm>() + CRC_SIZE;

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

impl SaveWalPtr {
  #[inline]
  pub(crate) fn new(wal_id: WalId, offset: WalOffset) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      offset: U64::new(offset),
    }
  }

  #[inline]
  pub(crate) fn to_array(&self) -> [u8; SAVE_SIZE] {
    to_disk(KIND_SAVE, self)
  }
}

impl Rotate {
  #[inline]
  pub(crate) fn new(wal_id: WalId) -> Self {
    Self {
      wal_id: U64::new(wal_id),
    }
  }

  #[inline]
  pub(crate) fn to_array(&self) -> [u8; ROTATE_SIZE] {
    to_disk(KIND_ROTATE, self)
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

  #[inline]
  pub(crate) fn to_array(&self) -> [u8; SST_ADD_SIZE] {
    to_disk(KIND_SST_ADD, self)
  }
}

impl SstRm {
  #[inline]
  pub(crate) fn new(id: u64) -> Self {
    Self { id: U64::new(id) }
  }

  #[inline]
  pub(crate) fn to_array(&self) -> [u8; SST_RM_SIZE] {
    to_disk(KIND_SST_RM, self)
  }
}
