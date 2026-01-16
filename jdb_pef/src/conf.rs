//! Default configuration for Elias-Fano and Select Index.
//! Elias-Fano 和 Select 索引的默认配置。

/// L1 sampling rate for Select Index.
/// Select 索引的 L1 采样率。
/// Used to store absolute positions.
/// 用于存储绝对位置。
pub const DEFAULT_L1_RATE: usize = 4724;

/// L2 sampling rate for Select Index.
/// Select 索引的 L2 采样率。
/// Used to store relative offsets within L1 blocks.
/// 用于存储 L1 块内的相对偏移。
/// Lower value means faster Select but slightly more memory.
/// 较低的值意味着更快的 Select 但稍微占用更多内存。
/// 16 is optimized for "single-word scan" (32-bit average gap).
pub const DEFAULT_L2_RATE: usize = 5;

/// Default block size for Partitioned Elias-Fano.
/// 分区 Elias-Fano 的默认块大小。
/// Trade-off between random access (smaller is better) and space/iteration (larger is better).
pub const DEFAULT_BLOCK_SIZE: usize = 3087;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conf {
  /// Sampling rate for L1 index (absolute position).
  pub l1_rate: usize,
  /// Sampling rate for L2 index (relative offset).
  pub l2_rate: usize,
  /// Block size for Partitioned Elias-Fano chunks.
  pub block_size: usize,
}

impl Default for Conf {
  fn default() -> Self {
    Self {
      l1_rate: DEFAULT_L1_RATE,
      l2_rate: DEFAULT_L2_RATE,
      block_size: DEFAULT_BLOCK_SIZE,
    }
  }
}
