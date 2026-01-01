//! WAL position
//! WAL 位置

use std::hash::Hash;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, byteorder::little_endian::U64};

use crate::Flag;

/// Val position for direct read (24B)
/// 直接读取的 val 位置（24字节）
///
/// Layout:
/// - wal_id: WAL file ID (8 bytes)
/// - offset_or_file_id: INFILE = val offset, FILE = file_id (8 bytes)
/// - len: val length (4 bytes)
/// - flag: storage flag (1 byte)
/// - _pad: reserved padding (3 bytes)
#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct Pos {
  wal_id: U64,
  offset_or_file_id: U64,
  len: u32,
  flag: u8,
  _pad: [u8; 3],
}

impl Hash for Pos {
  #[inline(always)]
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    // Only hash ID and location, ignoring length and flag/padding
    // 仅哈希 ID 和位置，忽略长度和标志/填充
    self.wal_id.hash(state);
    self.offset_or_file_id.hash(state);
  }
}

impl Pos {
  pub const SIZE: usize = 24;

  /// Create position with flag
  #[inline(always)]
  pub fn new(flag: Flag, wal_id: u64, offset_or_file_id: u64, len: u32) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      offset_or_file_id: U64::new(offset_or_file_id),
      len,
      flag: flag as u8,
      _pad: [0; 3],
    }
  }

  /// Create INFILE position
  #[inline(always)]
  pub fn infile(wal_id: u64, offset: u64, len: u32) -> Self {
    Self::new(Flag::Infile, wal_id, offset, len)
  }

  /// Create INFILE position with flag
  #[inline(always)]
  pub fn infile_with_flag(flag: Flag, wal_id: u64, offset: u64, len: u32) -> Self {
    Self::new(flag, wal_id, offset, len)
  }

  /// Create FILE position
  #[inline(always)]
  pub fn file(wal_id: u64, file_id: u64, len: u32) -> Self {
    Self::new(Flag::File, wal_id, file_id, len)
  }

  /// Create FILE position with flag
  #[inline(always)]
  pub fn file_with_flag(flag: Flag, wal_id: u64, file_id: u64, len: u32) -> Self {
    Self::new(flag, wal_id, file_id, len)
  }

  /// Create tombstone position
  #[inline(always)]
  pub fn tombstone(wal_id: u64, offset: u64) -> Self {
    Self::new(Flag::Tombstone, wal_id, offset, 0)
  }

  /// Get flag
  #[inline(always)]
  pub fn flag(&self) -> Flag {
    Flag::from_u8(self.flag)
  }

  /// Is INFILE mode
  #[inline(always)]
  pub fn is_infile(&self) -> bool {
    self.flag().is_infile()
  }

  /// Is tombstone
  #[inline(always)]
  pub fn is_tombstone(&self) -> bool {
    self.flag().is_tombstone()
  }

  /// Get WAL file ID
  #[inline(always)]
  pub fn id(&self) -> u64 {
    self.wal_id.get()
  }

  /// Get val offset (INFILE mode)
  #[inline(always)]
  pub fn offset(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get file ID (FILE mode)
  #[inline(always)]
  pub fn file_id(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get val length
  #[inline(always)]
  pub fn len(&self) -> u32 {
    self.len
  }

  /// Is empty
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }
}

const _: () = assert!(size_of::<Pos>() == 24);
