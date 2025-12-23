//! System configuration 系统配置

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelConfig {
  /// Data storage root 数据存储根目录
  pub data_dir: PathBuf,
  /// WAL directory WAL 目录
  pub wal_dir: PathBuf,
  /// Virtual node count 虚拟节点数量
  pub vnode_count: u16,
  /// Worker threads (0=auto) 工作线程数（0=自动）
  pub worker_threads: usize,
  /// IO queue depth IO 队列深度
  pub io_depth: u32,
  /// Block cache size in bytes 块缓存大小（字节）
  pub block_cache_size: u64,
}

impl Default for KernelConfig {
  fn default() -> Self {
    Self {
      data_dir: PathBuf::from("./data"),
      wal_dir: PathBuf::from("./wal"),
      vnode_count: 256,
      worker_threads: 0,
      io_depth: 128,
      block_cache_size: 1024 * 1024 * 1024, // 1GB
    }
  }
}
