use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Flag for storage and compression / 存储和压缩标志
///
/// | Value | Meaning           |
/// |-------|-------------------|
/// | 0x00  | INLINE (无压缩)    |
/// | 0x01  | INFILE + NONE     |
/// | 0x02  | INFILE + LZ4      |
/// | 0x03  | INFILE + ZSTD     |
/// | 0x04  | FILE + NONE       |
/// | 0x05  | FILE + LZ4        |
/// | 0x06  | FILE + ZSTD       |
/// | 0x80  | TOMBSTONE (删除)   |
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout, Default,
)]
#[repr(transparent)]
pub struct Flag(u8);

impl Flag {
  pub const INLINE: Self = Self(0x00);
  pub const INFILE: Self = Self(0x01);
  pub const INFILE_LZ4: Self = Self(0x02);
  pub const INFILE_ZSTD: Self = Self(0x03);
  pub const FILE: Self = Self(0x04);
  pub const FILE_LZ4: Self = Self(0x05);
  pub const FILE_ZSTD: Self = Self(0x06);
  pub const TOMBSTONE: Self = Self(0x80);

  /// Check if inline / 检查是否内联
  #[inline(always)]
  pub const fn is_inline(self) -> bool {
    self.0 == 0x00
  }

  /// Check if in same file / 检查是否同文件
  #[inline(always)]
  pub const fn is_infile(self) -> bool {
    matches!(self.0, 0x01..=0x03)
  }

  /// Check if external file / 检查是否外部文件
  #[inline(always)]
  pub const fn is_file(self) -> bool {
    matches!(self.0, 0x04..=0x06)
  }

  /// Check if compressed / 检查是否压缩
  #[inline(always)]
  pub const fn is_compressed(self) -> bool {
    matches!(self.0, 0x02 | 0x03 | 0x05 | 0x06)
  }

  /// Check if LZ4 / 检查是否 LZ4
  #[inline(always)]
  pub const fn is_lz4(self) -> bool {
    matches!(self.0, 0x02 | 0x05)
  }

  /// Check if ZSTD / 检查是否 ZSTD
  #[inline(always)]
  pub const fn is_zstd(self) -> bool {
    matches!(self.0, 0x03 | 0x06)
  }

  /// Check if tombstone / 检查是否为删除标记
  #[inline(always)]
  pub const fn is_tombstone(self) -> bool {
    (self.0 & 0x80) != 0
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
