//! SSTable configuration
//! SSTable 配置

/// Default values
/// 默认值
pub mod default {
  /// Block size (16KB, optimal for NVMe SSD)
  /// 块大小（16KB，NVMe SSD 最优）
  pub const BLOCK_SIZE: usize = 16384;

  /// PGM epsilon (error bound)
  /// PGM 误差范围
  pub const PGM_EPSILON: usize = 4;

  /// Restart interval
  /// 重启间隔
  pub const RESTART_INTERVAL: usize = 16;
}

/// SSTable configuration options
/// SSTable 配置选项
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Block size in bytes
  /// 块大小
  BlockSize(usize),

  /// PGM epsilon (error bound)
  /// PGM 误差范围
  PgmEpsilon(usize),

  /// Restart interval in block
  /// 块内重启点间隔
  RestartInterval(usize),
}
