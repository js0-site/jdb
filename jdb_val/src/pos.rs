//! WAL position / WAL 位置

use std::hash::Hash;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, byteorder::little_endian::U64};

/// WAL position (16B) / WAL 位置（16字节）
///
/// Explicit LittleEndian for cross-platform compatibility.
#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct Pos {
  wal_id: U64,
  offset: U64,
}

impl Hash for Pos {
  #[inline]
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_bytes().hash(state);
  }
}

impl Pos {
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

const _: () = assert!(size_of::<Pos>() == 16);

#[cfg(test)]
mod tests {
  use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
  };

  use super::*;

  #[test]
  fn test_new() {
    let pos = Pos::new(123, 456);
    assert_eq!(pos.id(), 123);
    assert_eq!(pos.pos(), 456);
  }

  #[test]
  fn test_default() {
    let pos = Pos::default();
    assert_eq!(pos.id(), 0);
    assert_eq!(pos.pos(), 0);
  }

  #[test]
  fn test_hash() {
    let p1 = Pos::new(1, 2);
    let p2 = Pos::new(1, 2);
    let p3 = Pos::new(1, 3);

    let mut h1 = DefaultHasher::new();
    let mut h2 = DefaultHasher::new();
    let mut h3 = DefaultHasher::new();

    p1.hash(&mut h1);
    p2.hash(&mut h2);
    p3.hash(&mut h3);

    assert_eq!(h1.finish(), h2.finish());
    assert_ne!(h1.finish(), h3.finish());
  }

  #[test]
  fn test_eq() {
    let p1 = Pos::new(1, 2);
    let p2 = Pos::new(1, 2);
    let p3 = Pos::new(2, 2);
    assert_eq!(p1, p2);
    assert_ne!(p1, p3);
  }
}
