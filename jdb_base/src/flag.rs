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
  /// Lookup table for fast conversion
  /// 快速转换查找表
  const VARIANTS: [Self; 9] = [
    Self::Infile,       // 0
    Self::InfileLz4,    // 1
    Self::InfileZstd,   // 2
    Self::InfileProbed, // 3
    Self::File,         // 4
    Self::FileLz4,      // 5
    Self::FileZstd,     // 6
    Self::FileProbed,   // 7
    Self::Tombstone,    // 8
  ];

  /// From u8
  /// 从 u8 转换
  #[inline]
  pub const fn from_u8(v: u8) -> Self {
    if v < 9 {
      Self::VARIANTS[v as usize]
    } else {
      Self::Tombstone
    }
  }

  /// To u8
  /// 转为 u8
  #[inline]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// Is infile mode (0-3)
  /// 是否为 WAL 内联模式 (0-3)
  #[inline]
  pub const fn is_infile(self) -> bool {
    (self as u8) < 4
  }

  /// Is file mode (4-7)
  /// 是否为独立文件模式 (4-7)
  #[inline]
  pub const fn is_file(self) -> bool {
    // 0b1100 mask: bit3=1 means >=8, bit2=1 means >=4
    // Only 4-7 have bit2=1 and bit3=0, so result == 4
    // 位掩码 0b1100：bit3=1 表示 >=8，bit2=1 表示 >=4
    // 只有 4-7 满足 bit2=1 且 bit3=0，结果为 4
    (self as u8) & 0b1100 == 4
  }

  /// Is tombstone
  /// 是否为删除标记
  #[inline]
  pub const fn is_tombstone(self) -> bool {
    (self as u8) == 8
  }

  /// Is compressed (LZ4 or Zstd)
  /// 是否已压缩
  #[inline]
  pub const fn is_compressed(self) -> bool {
    // LZ4=1,5 Zstd=2,6: low 2 bits are 01 or 10
    // LZ4=1,5 Zstd=2,6：低 2 位为 01 或 10
    let low = (self as u8) & 0b11;
    low == 1 || low == 2
  }

  /// Is LZ4 compressed
  /// 是否为 LZ4 压缩
  #[inline]
  pub const fn is_lz4(self) -> bool {
    // LZ4: 1, 5 (low 2 bits = 01)
    (self as u8) & 0b11 == 1
  }

  /// Is Zstd compressed
  /// 是否为 Zstd 压缩
  #[inline]
  pub const fn is_zstd(self) -> bool {
    // Zstd: 2, 6 (low 2 bits = 10)
    (self as u8) & 0b11 == 2
  }

  /// Is probed (incompressible)
  /// 是否已探测（不可压缩）
  #[inline]
  pub const fn is_probed(self) -> bool {
    // Probed: 3, 7 (low 2 bits = 11)
    (self as u8) & 0b11 == 3
  }

  /// To LZ4 variant
  /// 转为 LZ4 变体
  #[inline]
  pub const fn to_lz4(self) -> Self {
    // Set low 2 bits to 01, preserve higher bits (file/infile/tombstone)
    // 设置低 2 位为 01，保留高位（文件/内联/删除标记）
    Self::from_u8(((self as u8) & !0b11) | 1)
  }

  /// To Zstd variant
  /// 转为 Zstd 变体
  #[inline]
  pub const fn to_zstd(self) -> Self {
    // Set low 2 bits to 10, preserve higher bits (file/infile/tombstone)
    // 设置低 2 位为 10，保留高位（文件/内联/删除标记）
    Self::from_u8(((self as u8) & !0b11) | 2)
  }

  /// To probed variant
  /// 转为探测变体
  #[inline]
  pub const fn to_probed(self) -> Self {
    // Set low 2 bits to 11, preserve higher bits (file/infile/tombstone)
    // 设置低 2 位为 11，保留高位（文件/内联/删除标记）
    Self::from_u8(((self as u8) & !0b11) | 3)
  }
}
