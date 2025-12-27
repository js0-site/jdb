//! WAL location / WAL 位置

use std::hash::Hash;

use zerocopy::byteorder::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// WAL location (16B) / WAL 位置（16字节）
///
/// Explicit LittleEndian for cross-platform compatibility.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq)]
pub struct Loc {
  wal_id: U64,
  offset: U64,
}

impl Hash for Loc {
  #[inline]
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.wal_id.get().hash(state);
    self.offset.get().hash(state);
  }
}

impl Loc {
  pub const SIZE: usize = 16;

  #[inline(always)]
  pub fn new(wal_id: u64, offset: u64) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      offset: U64::new(offset),
    }
  }

  /// Get WAL file ID / 获取 WAL 文件 ID
  #[inline(always)]
  pub fn id(&self) -> u64 {
    self.wal_id.get()
  }

  /// Get offset / 获取偏移量
  #[inline(always)]
  pub fn pos(&self) -> u64 {
    self.offset.get()
  }
}

const _: () = assert!(size_of::<Loc>() == 16);
