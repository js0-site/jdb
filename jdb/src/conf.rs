//! Database configuration
//! 数据库配置

/// Database configuration
/// 数据库配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Memory table size threshold (bytes), default 64MB
  /// 内存表大小阈值（字节），默认 64MB
  MemThreshold(u64),
  /// File LRU cache capacity, default 16
  /// 文件 LRU 缓存容量，默认 16
  FileCap(usize),
  /// WAL configuration
  /// WAL 配置
  Wal(jdb_val::Conf),
  /// Checkpoint configuration
  /// 检查点配置
  Ckp(jdb_ckp::Conf),
}
