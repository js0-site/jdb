//! WAL position / WAL 位置

use std::hash::Hash;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, byteorder::little_endian::U64};

/// High bit mask for FILE mode / FILE 模式高位掩码
const FILE_MODE_BIT: u64 = 1 << 63;

/// Val position for direct read (24B)
/// 直接读取的 val 位置（24字节）
///
/// Layout:
/// - wal_id: high bit = mode (0=INFILE, 1=FILE), low 63 bits = wal_id
/// - offset_or_file_id: INFILE = val offset, FILE = file_id
/// - len: val length
#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct Pos {
  wal_id: U64,
  offset_or_file_id: U64,
  len: U64,
}

impl Hash for Pos {
  #[inline(always)]
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.wal_id.hash(state);
    self.offset_or_file_id.hash(state);
  }
}

impl Pos {
  pub const SIZE: usize = 24;

  /// Create INFILE position / 创建 INFILE 位置
  #[inline(always)]
  pub fn infile(wal_id: u64, offset: u64, len: u32) -> Self {
    Self {
      wal_id: U64::new(wal_id),
      offset_or_file_id: U64::new(offset),
      len: U64::new(len as u64),
    }
  }

  /// Create FILE position / 创建 FILE 位置
  #[inline(always)]
  pub fn file(wal_id: u64, file_id: u64, len: u32) -> Self {
    Self {
      wal_id: U64::new(wal_id | FILE_MODE_BIT),
      offset_or_file_id: U64::new(file_id),
      len: U64::new(len as u64),
    }
  }

  /// Check if INFILE mode / 检查是否 INFILE 模式
  #[inline(always)]
  pub fn is_infile(&self) -> bool {
    self.wal_id.get() & FILE_MODE_BIT == 0
  }

  /// Get WAL file ID / 获取 WAL 文件 ID
  #[inline(always)]
  pub fn id(&self) -> u64 {
    self.wal_id.get() & !FILE_MODE_BIT
  }

  /// Get val offset (INFILE mode) / 获取 val 偏移（INFILE 模式）
  #[inline(always)]
  pub fn offset(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get file ID (FILE mode) / 获取文件 ID（FILE 模式）
  #[inline(always)]
  pub fn file_id(&self) -> u64 {
    self.offset_or_file_id.get()
  }

  /// Get val length / 获取 val 长度
  #[inline(always)]
  pub fn len(&self) -> u32 {
    self.len.get() as u32
  }

  /// Check if empty / 检查是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len.get() == 0
  }
}

const _: () = assert!(size_of::<Pos>() == 24);

/// Record position for GC scan (16B)
/// GC 扫描的记录位置（16字节）
///
/// Points to Head (excludes magic)
/// 指向 Head（不含 magic）
#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct RecPos {
  wal_id: U64,
  head_offset: U64,
}

impl Hash for RecPos {
  #[inline(always)]
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.wal_id.hash(state);
    self.head_offset.hash(state);
  }
}

impl RecPos {
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

const _: () = assert!(size_of::<RecPos>() == 16);
