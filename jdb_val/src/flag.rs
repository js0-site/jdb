//! Storage flag
//! 存储标志
//!
//! ## Flag values (4 bits)
//! 标志值（4位）
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
//! | 8     | TOMBSTONE      | Deleted entry                            |

/// Storage flag (4 bits)
/// 存储标志（4位）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Flag {
  #[default]
  Infile = 0,
  InfileLz4 = 1,
  InfileZstd = 2,
  InfileProbed = 3,
  File = 4,
  FileLz4 = 5,
  FileZstd = 6,
  FileProbed = 7,
  Tombstone = 8,
}

impl Flag {
  const MASK: u8 = 0b1111;

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
      7 => Self::FileProbed,
      _ => Self::Tombstone,
    }
  }

  /// Check if tombstone
  /// 检查是否删除标记
  #[inline(always)]
  pub const fn is_tombstone(self) -> bool {
    matches!(self, Self::Tombstone)
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
    let v = self as u8;
    v >= 4 && v < 8
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
      Self::Tombstone => Self::Tombstone,
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
