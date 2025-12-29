//! Storage flag
//! 存储标志
//!
//! ## Flag byte layout
//! 标志字节布局
//!
//! | Bits  | Field | Description                               |
//! |-------|-------|-------------------------------------------|
//! | 0-2   | key   | Storage + compression for key             |
//! | 3-5   | val   | Storage + compression for val             |
//! | 6-7   | -     | Reserved                                  |
//!
//! ## Storage values (3 bits)
//! 存储值（3位）
//!
//! | Value | Name           | Description                              |
//! |-------|----------------|------------------------------------------|
//! | 0     | INFILE         | In same WAL file, no compression         |
//! | 1     | INFILE_LZ4     | In same WAL file, LZ4 compressed         |
//! | 2     | INFILE_ZSTD    | In same WAL file, ZSTD compressed        |
//! | 3     | INFILE_PROBED  | In same WAL file, tested incompressible  |
//! | 4     | FILE           | Separate file, no compression            |
//! | 5     | FILE_LZ4       | Separate file, LZ4 compressed            |
//! | 6     | FILE_ZSTD      | Separate file, ZSTD compressed           |
//! | 7     | FILE_PROBED    | Separate file, tested incompressible     |
//!
//! 中文说明：
//! - INFILE: 数据存储在同一个 WAL 文件中
//! - FILE: 数据存储在独立文件中
//! - LZ4/ZSTD: 压缩算法
//! - PROBED: 已测试过，数据不可压缩（压缩后反而更大）

/// Storage mode (3 bits)
/// 存储模式（3位）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Store {
  #[default]
  Infile = 0,
  InfileLz4 = 1,
  InfileZstd = 2,
  InfileProbed = 3,
  File = 4,
  FileLz4 = 5,
  FileZstd = 6,
  FileProbed = 7,
}

impl Store {
  const MASK: u8 = 0b111;

  #[inline(always)]
  pub const fn from_u8(v: u8) -> Self {
    match v & Self::MASK {
      0 => Self::Infile,
      1 => Self::InfileLz4,
      2 => Self::InfileZstd,
      3 => Self::InfileProbed,
      4 => Self::File,
      5 => Self::FileLz4,
      6 => Self::FileZstd,
      _ => Self::FileProbed,
    }
  }

  /// Check if infile
  /// 检查是否同文件
  #[inline(always)]
  pub const fn is_infile(self) -> bool {
    (self as u8) < 4
  }

  /// Check if external file
  /// 检查是否外部文件
  #[inline(always)]
  pub const fn is_file(self) -> bool {
    (self as u8) >= 4
  }

  /// Check if compressed
  /// 检查是否压缩
  #[inline(always)]
  pub const fn is_compressed(self) -> bool {
    matches!(
      self,
      Self::InfileLz4 | Self::InfileZstd | Self::FileLz4 | Self::FileZstd
    )
  }

  /// Check if LZ4
  /// 检查是否 LZ4
  #[inline(always)]
  pub const fn is_lz4(self) -> bool {
    matches!(self, Self::InfileLz4 | Self::FileLz4)
  }

  /// Check if ZSTD
  /// 检查是否 ZSTD
  #[inline(always)]
  pub const fn is_zstd(self) -> bool {
    matches!(self, Self::InfileZstd | Self::FileZstd)
  }

  /// Check if probed (incompressible)
  /// 检查是否已探测不可压缩
  #[inline(always)]
  pub const fn is_probed(self) -> bool {
    matches!(self, Self::InfileProbed | Self::FileProbed)
  }

  /// Get probed version
  /// 获取已探测版本
  #[inline(always)]
  pub const fn to_probed(self) -> Self {
    match self {
      Self::Infile | Self::InfileLz4 | Self::InfileZstd | Self::InfileProbed => Self::InfileProbed,
      Self::File | Self::FileLz4 | Self::FileZstd | Self::FileProbed => Self::FileProbed,
    }
  }

  /// Get LZ4 version
  /// 获取 LZ4 版本
  #[inline(always)]
  pub const fn to_lz4(self) -> Self {
    if self.is_file() {
      Self::FileLz4
    } else {
      Self::InfileLz4
    }
  }

  /// Get ZSTD version
  /// 获取 ZSTD 版本
  #[inline(always)]
  pub const fn to_zstd(self) -> Self {
    if self.is_file() {
      Self::FileZstd
    } else {
      Self::InfileZstd
    }
  }
}

/// Combined flag for key and val
/// key 和 val 的组合标志
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct Flag(u8);

impl Flag {
  const VAL_SHIFT: u8 = 3;

  /// Create flag from key and val store
  /// 从 key 和 val 存储创建标志
  #[inline(always)]
  pub const fn new(key: Store, val: Store) -> Self {
    Self((key as u8) | ((val as u8) << Self::VAL_SHIFT))
  }

  /// Get key store
  /// 获取 key 存储
  #[inline(always)]
  pub const fn key(self) -> Store {
    Store::from_u8(self.0)
  }

  /// Get val store
  /// 获取 val 存储
  #[inline(always)]
  pub const fn val(self) -> Store {
    Store::from_u8(self.0 >> Self::VAL_SHIFT)
  }

  /// Create from raw byte
  /// 从原始字节创建
  #[inline(always)]
  pub const fn from_u8(v: u8) -> Self {
    Self(v)
  }

  /// Get raw byte
  /// 获取原始字节
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self.0
  }
}

impl From<u8> for Flag {
  #[inline(always)]
  fn from(v: u8) -> Self {
    Self(v)
  }
}

impl From<Flag> for u8 {
  #[inline(always)]
  fn from(f: Flag) -> Self {
    f.0
  }
}
