// Configuration for jdb
// jdb 配置模块

/// Configuration item enum
/// 配置项枚举
pub enum ConfItem {
  /// Memtable size threshold in bytes (default: 64MB)
  /// Memtable 大小阈值（字节，默认 64MB）
  MemtableSize(u64),
  /// L0 SSTable count threshold for compaction (default: 4)
  /// L0 层 SSTable 数量阈值（默认 4）
  L0Threshold(usize),
  /// Level size ratio for compaction (default: 10)
  /// 层级大小比例（默认 10）
  LevelRatio(usize),
  /// Block size in bytes (default: 4KB)
  /// 块大小（字节，默认 4KB）
  BlockSize(usize),
  /// Restart interval for prefix compression (default: 16)
  /// 前缀压缩重启间隔（默认 16）
  RestartInterval(usize),
  /// Cuckoo filter false positive rate (default: 0.01)
  /// 布谷鸟过滤器假阳性率（默认 0.01）
  FilterFpr(f64),
}

/// Database configuration
/// 数据库配置
#[derive(Clone)]
pub struct Conf {
  pub memtable_size: u64,
  pub l0_threshold: usize,
  pub level_ratio: usize,
  pub block_size: usize,
  pub restart_interval: usize,
  pub filter_fpr: f64,
}

impl Default for Conf {
  fn default() -> Self {
    Self {
      memtable_size: 64 * 1024 * 1024, // 64MB
      l0_threshold: 4,
      level_ratio: 10,
      block_size: 4 * 1024, // 4KB
      restart_interval: 16,
      filter_fpr: 0.01,
    }
  }
}

impl Conf {
  /// Create configuration from items
  /// 从配置项创建配置
  pub fn from_items(items: &[ConfItem]) -> Self {
    let mut conf = Self::default();
    for item in items {
      match item {
        ConfItem::MemtableSize(v) => conf.memtable_size = *v,
        ConfItem::L0Threshold(v) => conf.l0_threshold = *v,
        ConfItem::LevelRatio(v) => conf.level_ratio = *v,
        ConfItem::BlockSize(v) => conf.block_size = *v,
        ConfItem::RestartInterval(v) => conf.restart_interval = *v,
        ConfItem::FilterFpr(v) => conf.filter_fpr = *v,
      }
    }
    conf
  }
}
