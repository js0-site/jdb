//! Level configuration
//! 层级配置

/// Max supported levels (0-7)
/// 最大支持层数 (0-7)
pub const MAX_LEVELS: usize = 8;

/// Max level number
/// 最大层级号
pub const MAX_LEVEL: u8 = 7;

const MB: u64 = 1024 * 1024;

/// Default configuration values (RocksDB style)
/// 默认配置值（RocksDB 风格）
pub mod default {
  pub const L0_LIMIT: usize = 4;
  pub const BASE_MB: u16 = 256; // 256MB
  pub const RATIO: u8 = 10;
}

/// Level manager configuration
/// 层级管理器配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// L0 file count threshold
  /// L0 文件数阈值
  L0Limit(usize),
  /// Base size in MB
  /// 基准大小（MB）
  BaseMb(u16),
  /// Size ratio between levels
  /// 层级间大小比例
  Ratio(u8),
}

/// Parsed configuration
/// 解析后的配置
#[derive(Debug, Clone, Copy)]
pub struct ParsedConf {
  pub l0_limit: usize,
  pub base_size: u64,
  pub ratio: u64,
}

impl Default for ParsedConf {
  fn default() -> Self {
    Self {
      l0_limit: default::L0_LIMIT,
      base_size: default::BASE_MB as u64 * MB,
      ratio: default::RATIO as u64,
    }
  }
}

impl ParsedConf {
  pub fn new(conf: &[Conf]) -> Self {
    let mut c = Self::default();
    for item in conf {
      match *item {
        Conf::L0Limit(v) => c.l0_limit = v.max(1),
        Conf::BaseMb(v) => c.base_size = v.max(1) as u64 * MB,
        Conf::Ratio(v) => c.ratio = v.max(2) as u64,
      }
    }
    c
  }
}
