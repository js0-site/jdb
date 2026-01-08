//! WAL position
//! WAL 位置

use std::hash::{Hash, Hasher};

use zerocopy::{
  FromBytes, Immutable, IntoBytes, KnownLayout,
  little_endian::{U32, U64},
};

use crate::Flag;

/// Val position for direct read (32B)
/// 直接读取的 val 位置（32字节）
///
/// Layout:
/// - ver: version/sequence number for merge ordering (8 bytes)
/// - wal_id: WAL file ID (8 bytes, Little Endian)
/// - offset_or_file_id: INFILE = val offset, FILE = file_id (8 bytes, Little Endian)
/// - len: val length (4 bytes)
/// - flag: storage flag (1 byte)
/// - _pad: reserved padding (3 bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Pos {
  ver: U64,
  wal_id: U64,
  offset_or_file_id: U64,
  len: U32,
  flag: u8,
  _pad: [u8; 3],
}

impl PartialEq for Pos {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.ver == other.ver
      && self.wal_id == other.wal_id
      && self.offset_or_file_id == other.offset_or_file_id
      && self.len == other.len
      && self.flag == other.flag
  }
}

impl Eq for Pos {}

impl Hash for Pos {
  #[inline]
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.ver.hash(state);
    self.wal_id.hash(state);
    self.offset_or_file_id.hash(state);
    self.len.hash(state);
    self.flag.hash(state);
  }
}

impl Pos {
  pub const SIZE: usize = 32;

  /// Create position / 创建位置
  #[inline]
  pub fn new(ver: u64, flag: Flag, wal_id: u64, offset_or_file_id: u64, len: u32) -> Self {
    Self {
      ver: U64::new(ver),
      wal_id: U64::new(wal_id),
      offset_or_file_id: U64::new(offset_or_file_id),
      len: U32::new(len),
      flag: flag.as_u8(),
      _pad: [0; 3],
    }
  }

  /// Convert to tombstone, preserving storage info
  /// 转为墓碑，保留存储信息
  #[inline]
  pub fn to_tombstone(self) -> Self {
    Self {
      flag: self.flag | 0x08,
      ..self
    }
  }

  /// Get original storage flag (clear tombstone bit)
  /// 获取原始存储标志（清除墓碑位）
  #[inline]
  pub const fn storage(&self) -> Flag {
    Flag::from_u8(self.flag & !0x08)
  }

  /// Get version number
  /// 获取版本号
  #[inline]
  pub const fn ver(&self) -> u64 {
    self.ver.get()
  }

  /// Get flag
  /// 获取标志
  #[inline]
  pub const fn flag(&self) -> Flag {
    Flag::from_u8(self.flag)
  }

  /// Is INFILE mode (bit2 clear)
  /// 是否为 INFILE 模式（bit2 清零）
  #[inline]
  pub const fn is_infile(&self) -> bool {
    self.flag & 0x04 == 0
  }

  /// Is FILE mode (bit2 set, excludes tombstone check)
  /// 是否为 FILE 模式（bit2 置位，不检查墓碑）
  #[inline]
  pub const fn is_file(&self) -> bool {
    self.flag & 0x04 != 0
  }

  /// Is tombstone (bit3 set)
  /// 是否为删除标记（bit3 置位）
  #[inline]
  pub const fn is_tombstone(&self) -> bool {
    self.flag & 0x08 != 0
  }

  /// Get WAL file ID
  /// 获取 WAL 文件 ID
  #[inline]
  pub const fn wal_id(&self) -> u64 {
    self.wal_id.get()
  }

  /// Get val offset (INFILE mode)
  /// 获取值偏移量（INFILE 模式）
  #[inline]
  pub const fn offset(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get file ID (FILE mode)
  /// 获取文件 ID（FILE 模式）
  #[inline]
  pub const fn file_id(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get val length
  /// 获取值长度
  #[inline]
  pub const fn len(&self) -> u32 {
    self.len.get()
  }

  /// Is empty
  /// 是否为空
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.len.get() == 0
  }
}

const _: () = assert!(size_of::<Pos>() == 32);
