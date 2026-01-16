use std::hash::Hash;

use bitcode::{Decode, Encode};

use crate::flag::Flag;

#[derive(Encode, Decode, Hash, Clone, Copy, Eq, PartialEq)]
pub struct Pos {
  ver: u64,
  wal_id: u64,
  offset_or_file_id: u64,
  len: u32,
  flag: u8,
}

impl Pos {
  /// Create position / 创建位置
  #[inline]
  pub fn new(ver: u64, flag: Flag, wal_id: u64, offset_or_file_id: u64, len: u32) -> Self {
    Self {
      ver,
      wal_id,
      offset_or_file_id,
      len,
      flag: flag.into(),
    }
  }

  /// Convert to tombstone, preserving storage info
  /// 转为墓碑，保留存储信息
  #[inline]
  pub fn tombstone(self) -> Self {
    Self {
      flag: self.flag | 0x08,
      ..self
    }
  }

  /// Get original storage flag (clear tombstone bit)
  /// 获取原始存储标志（清除墓碑位）
  #[inline]
  pub const fn storage(&self) -> Flag {
    Flag::new(self.flag & !0x08)
  }

  /// Get version number
  /// 获取版本号
  #[inline]
  pub const fn ver(&self) -> u64 {
    self.ver
  }

  /// Get flag
  /// 获取标志
  #[inline]
  pub const fn flag(&self) -> Flag {
    Flag::new(self.flag)
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
    self.wal_id
  }

  /// Get val offset (INFILE mode)
  /// 获取值偏移量（INFILE 模式）
  #[inline]
  pub const fn offset(&self) -> u64 {
    self.offset_or_file_id
  }

  /// Get file ID (FILE mode)
  /// 获取文件 ID（FILE 模式）
  #[inline]
  pub const fn file_id(&self) -> u64 {
    self.offset_or_file_id
  }

  /// Get val length
  /// 获取值长度
  #[inline]
  pub const fn len(&self) -> u32 {
    self.len
  }

  /// Is empty
  /// 是否为空
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// Size of the Pos struct in bytes
  /// Pos 结构体的大小（字节）
  pub const SIZE: usize = std::mem::size_of::<Self>();
}
