//! Storage flags
//! 存储标志
//!
//! ## Flag values (4 bits)
//! | Value | Name         | Description                    |
//! |-------|--------------|--------------------------------|
//! | 0     | Infile       | In WAL file, no compression    |
//! | 1     | InfileLz4    | In WAL file, LZ4 compressed    |
//! | 2     | InfileZstd   | In WAL file, Zstd compressed   |
//! | 3     | InfileProbed | In WAL file, incompressible    |
//! | 4     | File         | Separate file, no compression  |
//! | 5     | FileLz4      | Separate file, LZ4 compressed  |
//! | 6     | FileZstd     | Separate file, Zstd compressed |
//! | 7     | FileProbed   | Separate file, incompressible  |
//! | 8     | Tombstone    | Deleted entry                  |

/// Storage flag (4 bits)
/// 存储标志（4位）
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

  /// From u8
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

  /// Is infile mode (0-3)
  #[inline(always)]
  pub const fn is_infile(self) -> bool {
    (self as u8) < 4
  }

  /// Is file mode (4-7)
  #[inline(always)]
  pub const fn is_file(self) -> bool {
    let v = self as u8;
    v >= 4 && v < 8
  }

  /// Is tombstone
  #[inline(always)]
  pub const fn is_tombstone(self) -> bool {
    matches!(self, Self::Tombstone)
  }

  /// Is compressed (LZ4 or Zstd)
  #[inline(always)]
  pub const fn is_compressed(self) -> bool {
    matches!(
      self,
      Self::InfileLz4 | Self::InfileZstd | Self::FileLz4 | Self::FileZstd
    )
  }

  /// Is LZ4 compressed
  #[inline(always)]
  pub const fn is_lz4(self) -> bool {
    matches!(self, Self::InfileLz4 | Self::FileLz4)
  }

  /// Is Zstd compressed
  #[inline(always)]
  pub const fn is_zstd(self) -> bool {
    matches!(self, Self::InfileZstd | Self::FileZstd)
  }

  /// Is probed (incompressible)
  #[inline(always)]
  pub const fn is_probed(self) -> bool {
    matches!(self, Self::InfileProbed | Self::FileProbed)
  }

  /// To LZ4 variant
  #[inline(always)]
  pub const fn to_lz4(self) -> Self {
    if self.is_file() {
      Self::FileLz4
    } else {
      Self::InfileLz4
    }
  }

  /// To Zstd variant
  #[inline(always)]
  pub const fn to_zstd(self) -> Self {
    if self.is_file() {
      Self::FileZstd
    } else {
      Self::InfileZstd
    }
  }

  /// To probed variant
  #[inline(always)]
  pub const fn to_probed(self) -> Self {
    match self {
      Self::Infile | Self::InfileLz4 | Self::InfileZstd | Self::InfileProbed => Self::InfileProbed,
      Self::File | Self::FileLz4 | Self::FileZstd | Self::FileProbed => Self::FileProbed,
      Self::Tombstone => Self::Tombstone,
    }
  }
}
