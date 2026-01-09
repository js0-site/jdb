//! Storage flags
//! 存储标志
//!
//! ## Flag layout (4 bits)
//! ```text
//! bit3: tombstone (0x08)
//! bit2: file mode (0x04)
//! bit1-0: compression (00=none, 01=lz4, 10=zstd, 11=probed)
//! ```
//!
//! | Value | Binary | Description                    |
//! |-------|--------|--------------------------------|
//! | 0     | 0000   | Infile, no compression         |
//! | 1     | 0001   | Infile, LZ4                    |
//! | 2     | 0010   | Infile, Zstd                   |
//! | 3     | 0011   | Infile, probed                 |
//! | 4     | 0100   | File, no compression           |
//! | 5     | 0101   | File, LZ4                      |
//! | 6     | 0110   | File, Zstd                     |
//! | 7     | 0111   | File, probed                   |
//! | 8-15  | 1xxx   | Tombstone (preserves storage)  |

// Bit masks / 位掩码
const TOMBSTONE_BIT: u8 = 0x08;
const FILE_BIT: u8 = 0x04;
const COMPRESS_MASK: u8 = 0x03;

/// Storage flag (4 bits)
/// 存储标志（4位）
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Flag(u8);

// Common flags / 常用标志
impl Flag {
  pub const INFILE: Self = Self(0);
  pub const INFILE_LZ4: Self = Self(1);
  pub const INFILE_ZSTD: Self = Self(2);
  pub const INFILE_PROBED: Self = Self(3);
  pub const FILE: Self = Self(FILE_BIT);
  pub const FILE_LZ4: Self = Self(FILE_BIT | 1);
  pub const FILE_ZSTD: Self = Self(FILE_BIT | 2);
  pub const FILE_PROBED: Self = Self(FILE_BIT | 3);
}

impl Flag {
  /// From u8 / 从 u8 转换
  #[inline]
  pub const fn from_u8(v: u8) -> Self {
    Self(v & 0x0F)
  }

  /// To u8 / 转为 u8
  #[inline]
  pub const fn as_u8(self) -> u8 {
    self.0
  }

  /// Is infile mode / 是否为 WAL 内联模式
  #[inline]
  pub const fn is_infile(self) -> bool {
    self.0 & FILE_BIT == 0
  }

  /// Is file mode / 是否为独立文件模式
  #[inline]
  pub const fn is_file(self) -> bool {
    self.0 & FILE_BIT != 0
  }

  /// Is tombstone / 是否为删除标记
  #[inline]
  pub const fn is_tombstone(self) -> bool {
    self.0 & TOMBSTONE_BIT != 0
  }

  /// Set tombstone bit / 设置删除标记位
  #[inline]
  pub const fn to_tombstone(self) -> Self {
    Self(self.0 | TOMBSTONE_BIT)
  }

  /// Clear tombstone bit, get original storage flag
  /// 清除删除标记位，获取原始存储标志
  #[inline]
  pub const fn storage(self) -> Self {
    Self(self.0 & !TOMBSTONE_BIT)
  }

  /// Is compressed (LZ4 or Zstd) / 是否已压缩
  #[inline]
  pub const fn is_compressed(self) -> bool {
    let c = self.0 & COMPRESS_MASK;
    c == 1 || c == 2
  }

  /// Is LZ4 / 是否为 LZ4
  #[inline]
  pub const fn is_lz4(self) -> bool {
    self.0 & COMPRESS_MASK == 1
  }

  /// Is Zstd / 是否为 Zstd
  #[inline]
  pub const fn is_zstd(self) -> bool {
    self.0 & COMPRESS_MASK == 2
  }

  /// Is probed (incompressible) / 是否已探测
  #[inline]
  pub const fn is_probed(self) -> bool {
    self.0 & COMPRESS_MASK == 3
  }

  /// To LZ4 variant / 转为 LZ4 变体
  #[inline]
  pub const fn to_lz4(self) -> Self {
    Self((self.0 & !COMPRESS_MASK) | 1)
  }

  /// To Zstd variant / 转为 Zstd 变体
  #[inline]
  pub const fn to_zstd(self) -> Self {
    Self((self.0 & !COMPRESS_MASK) | 2)
  }

  /// To probed variant / 转为探测变体
  #[inline]
  pub const fn to_probed(self) -> Self {
    Self((self.0 & !COMPRESS_MASK) | 3)
  }
}
