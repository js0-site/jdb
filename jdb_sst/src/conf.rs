//! SSTable configuration
//! SSTable 配置

use crate::compress::Compress;

/// SSTable configuration options
/// SSTable 配置选项
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// PGM epsilon (error bound)
  /// PGM 误差范围
  PgmEpsilon(usize),

  /// Restart interval in block
  /// 块内重启点间隔
  RestartInterval(usize),

  /// Compression configuration
  /// 压缩配置
  Compress(Compress),

  /// File cache size (number of file handles)
  /// 文件缓存大小 (文件句柄数量)
  FileCacheSize(usize),

  /// Block size
  /// 块大小
  BlockSize(usize),
}

/// Internal configuration struct
/// 内部配置结构体
#[derive(Debug, Clone)]
pub struct Config {
  /// PGM epsilon
  /// PGM 误差范围
  pub pgm_epsilon: usize,

  /// Restart interval
  /// 重启间隔
  pub restart_interval: usize,

  /// Compression configuration (fixed size 7)
  /// 压缩配置 (固定大小 7)
  pub compress: [Compress; 7],

  pub block_size: usize, // 块大小
  /// File cache size
  /// 文件缓存大小
  pub file_cache_size: usize,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      pgm_epsilon: default::PGM_EPSILON,
      restart_interval: default::RESTART_INTERVAL,
      compress: default::COMPRESS,
      file_cache_size: default::FILE_CACHE_SIZE,
      block_size: default::BLOCK_SIZE,
    }
  }
}

impl From<&[Conf]> for Config {
  fn from(conf_li: &[Conf]) -> Self {
    let mut config = Self::default();
    for &conf in conf_li {
      match conf {
        Conf::PgmEpsilon(v) => config.pgm_epsilon = v,
        Conf::RestartInterval(v) => config.restart_interval = v,
        Conf::Compress(v) => {
          if (v.sst_level as usize) < config.compress.len() {
            config.compress[v.sst_level as usize] = v;
          } else {
            log::warn!("Compress sst_level {:?} out of bounds (0-6)", v.sst_level);
          }
        }
        Conf::FileCacheSize(v) => config.file_cache_size = v,
        Conf::BlockSize(v) => config.block_size = v,
      }
    }
    config
  }
}

/// Default values
/// 默认值
pub mod default {
  use jdb_base::sst::Level;

  use crate::compress::{Compress, CompressAlgo};

  pub const KB: usize = 1024;
  pub const MB: usize = 1024 * KB;
  pub const BLOCK_SIZE: usize = 8 * MB;

  /// PGM epsilon (error bound)
  /// PGM 误差范围
  pub const PGM_EPSILON: usize = 32;

  /// Restart interval
  /// 重启间隔
  pub const RESTART_INTERVAL: usize = 16;

  /// Default compression
  /// 默认压缩
  pub const COMPRESS: [Compress; 7] = [
    Compress::new(Level::L0, CompressAlgo::None, 0),
    Compress::new(Level::L1, CompressAlgo::Lz4, 1),
    Compress::new(Level::L2, CompressAlgo::Lz4, 1),
    Compress::new(Level::L3, CompressAlgo::Zstd, 1),
    Compress::new(Level::L4, CompressAlgo::Zstd, 3),
    Compress::new(Level::L5, CompressAlgo::Zstd, 7),
    Compress::new(Level::L6, CompressAlgo::Zstd, 9),
  ];

  /// Default file cache size
  /// 默认文件缓存大小
  pub const FILE_CACHE_SIZE: usize = 512;
}
