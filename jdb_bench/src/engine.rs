// Engine adapter trait for unified benchmarking
// 统一的引擎适配器 trait

use std::{future::Future, path::Path};

use crate::Result;

/// Calculate directory size iteratively / 迭代计算目录大小
pub fn dir_size(path: &Path) -> u64 {
  if !path.exists() {
    return 0;
  }
  let mut total = 0u64;
  let mut stack = vec![path.to_path_buf()];

  while let Some(current_path) = stack.pop() {
    if let Ok(entries) = std::fs::read_dir(&current_path) {
      for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
          stack.push(p);
        } else {
          total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
      }
    }
  }
  total
}

/// Get current process memory usage in bytes / 获取当前进程内存使用量（字节）
pub fn process_memory() -> u64 {
  memory_stats::memory_stats()
    .map(|s| s.physical_mem as u64)
    .unwrap_or(0)
}

/// Unified engine adapter trait / 统一引擎适配器 trait
///
/// Provides common interface for benchmarking different storage engines.
/// 为不同存储引擎提供统一的基准测试接口。
///
/// Note: No Send bound since compio is single-threaded async runtime.
/// 注意：无 Send 约束，因为 compio 是单线程异步运行时。
pub trait BenchEngine {
  /// Engine name / 引擎名称
  fn name(&self) -> &str;

  /// Data directory path / 数据目录路径
  fn data_path(&self) -> &Path;

  /// Put key-value / 写入键值
  fn put(&mut self, key: &[u8], val: &[u8]) -> impl Future<Output = Result<()>>;

  /// Get value by key / 按键读取
  fn get(&mut self, key: &[u8]) -> impl Future<Output = Result<Option<Vec<u8>>>>;

  /// Delete key / 删除键
  fn del(&mut self, key: &[u8]) -> impl Future<Output = Result<()>>;

  /// Sync to disk / 同步到磁盘
  fn sync(&mut self) -> impl Future<Output = Result<()>>;

  /// Get disk usage in bytes / 获取磁盘使用量（字节）
  fn disk_usage(&self) -> u64 {
    dir_size(self.data_path())
  }

  /// Get memory usage in bytes / 获取内存使用量（字节）
  fn memory_usage(&self) -> u64 {
    process_memory()
  }
}
