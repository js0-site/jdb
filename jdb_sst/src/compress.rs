//! Compression types and levels
//! 压缩类型和等级

use jdb_base::sst::Level;

/// Compression algorithm
/// 压缩算法
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CompressAlgo {
  /// No compression
  /// 不压缩
  #[default]
  None = 0,
  /// LZ4 compression
  /// LZ4 压缩
  Lz4 = 1,
  /// ZSTD compression
  /// ZSTD 压缩
  Zstd = 2,
}

/// Compression level
/// 压缩等级
pub type CompressLevel = u8;

/// Compression configuration
/// 压缩配置
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compress {
  /// SSTable level
  /// SST 层级
  pub sst_level: Level,
  /// Compression algorithm
  /// 压缩算法
  pub algo: CompressAlgo,
  /// Compression level
  /// 压缩等级
  pub compress_level: CompressLevel,
}

impl Compress {
  /// Create new compression configuration
  /// 创建新的压缩配置
  pub const fn new(sst_level: Level, algo: CompressAlgo, compress_level: CompressLevel) -> Self {
    Self {
      sst_level,
      algo,
      compress_level,
    }
  }
}
