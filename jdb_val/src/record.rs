//! Record position for GC scan (16B)
//! GC 扫描的记录位置（16字节）
//!
//! Points to Head (excludes magic)
//! 指向 Head（不含 magic）

use std::hash::Hash;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, byteorder::little_endian::U64};

#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct Record {
  wal_id: U64,
  head_offset: U64,
}

impl Hash for Record {
  #[inline(always)]
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.wal_id.hash(state);
    self.head_offset.hash(state);
  }
}

impl Record {
  pub const SIZE: usize = 16;

  #[inline(always)]
  pub fn new(wal_id: u64, head_offset: u64) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      head_offset: U64::new(head_offset),
    }
  }

  /// Get WAL file ID / 获取 WAL 文件 ID
  #[inline(always)]
  pub fn id(&self) -> u64 {
    self.wal_id.get()
  }

  /// Get head offset / 获取 head 偏移
  #[inline(always)]
  pub fn offset(&self) -> u64 {
    self.head_offset.get()
  }
}

const _: () = assert!(size_of::<Record>() == 16);
