//! WAL position
//! WAL 位置

use std::hash::{Hash, Hasher};

use zerocopy::{
  FromBytes, Immutable, IntoBytes, KnownLayout,
  little_endian::{U32, U64},
};

use crate::Flag;

/// Val position for direct read (24B)
/// 直接读取的 val 位置（24字节）
///
/// Layout:
/// - wal_id: WAL file ID (8 bytes, Little Endian)
/// - offset_or_file_id: INFILE = val offset, FILE = file_id (8 bytes, Little Endian)
/// - len: val length (4 bytes)
/// - flag: storage flag (1 byte)
/// - _pad: reserved padding (3 bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Pos {
  wal_id: U64,
  offset_or_file_id: U64,
  len: U32,
  flag: u8,
  _pad: [u8; 3],
}

impl PartialEq for Pos {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    // Compare fields directly (faster than slice, O(1) register ops)
    // 直接比较字段（比切片更快，O(1) 寄存器操作）
    self.wal_id == other.wal_id
      && self.offset_or_file_id == other.offset_or_file_id
      && self.len == other.len
      && self.flag == other.flag
  }
}

impl Eq for Pos {}

impl Hash for Pos {
  #[inline]
  fn hash<H: Hasher>(&self, state: &mut H) {
    // Hash fields directly, MUST match PartialEq
    // 直接哈希字段，必须与 PartialEq 保持一致
    self.wal_id.hash(state);
    self.offset_or_file_id.hash(state);
    self.len.hash(state);
    self.flag.hash(state);
  }
}

impl Pos {
  pub const SIZE: usize = 24;

  /// Create position with flag
  /// 创建带标志的位置
  #[inline]
  pub fn new(flag: Flag, wal_id: u64, offset_or_file_id: u64, len: u32) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      offset_or_file_id: U64::new(offset_or_file_id),
      len: U32::new(len),
      flag: flag as u8,
      _pad: [0; 3],
    }
  }

  /// Create INFILE position
  /// 创建 INFILE 位置
  #[inline]
  pub fn infile(wal_id: u64, offset: u64, len: u32) -> Self {
    Self::new(Flag::Infile, wal_id, offset, len)
  }

  /// Create INFILE position with flag
  /// 创建带标志的 INFILE 位置
  #[inline]
  pub fn infile_with_flag(flag: Flag, wal_id: u64, offset: u64, len: u32) -> Self {
    Self::new(flag, wal_id, offset, len)
  }

  /// Create FILE position
  /// 创建 FILE 位置
  #[inline]
  pub fn file(wal_id: u64, file_id: u64, len: u32) -> Self {
    Self::new(Flag::File, wal_id, file_id, len)
  }

  /// Create FILE position with flag
  /// 创建带标志的 FILE 位置
  #[inline]
  pub fn file_with_flag(flag: Flag, wal_id: u64, file_id: u64, len: u32) -> Self {
    Self::new(flag, wal_id, file_id, len)
  }

  /// Create tombstone position
  /// 创建删除标记位置
  #[inline]
  pub fn tombstone(wal_id: u64, offset: u64) -> Self {
    Self::new(Flag::Tombstone, wal_id, offset, 0)
  }

  /// Get flag
  /// 获取标志
  #[inline]
  pub fn flag(&self) -> Flag {
    Flag::from_u8(self.flag)
  }

  /// Is INFILE mode (flag 0-3)
  /// 是否为 INFILE 模式（flag 0-3）
  #[inline]
  pub fn is_infile(&self) -> bool {
    self.flag < 4
  }

  /// Is FILE mode (flag 4-7)
  /// 是否为 FILE 模式（flag 4-7）
  #[inline]
  pub fn is_file(&self) -> bool {
    (self.flag & 0b1100) == 4
  }

  /// Is tombstone (flag 8)
  /// 是否为删除标记（flag 8）
  #[inline]
  pub fn is_tombstone(&self) -> bool {
    self.flag == 8
  }

  /// Get WAL file ID
  /// 获取 WAL 文件 ID
  #[inline]
  pub fn id(&self) -> u64 {
    self.wal_id.get()
  }

  /// Get val offset (INFILE mode)
  /// 获取值偏移量（INFILE 模式）
  #[inline]
  pub fn offset(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get file ID (FILE mode)
  /// 获取文件 ID（FILE 模式）
  #[inline]
  pub fn file_id(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get val length
  /// 获取值长度
  #[inline]
  pub fn len(&self) -> u32 {
    self.len.get()
  }

  /// Is empty
  /// 是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }
}

const _: () = assert!(size_of::<Pos>() == 24);
