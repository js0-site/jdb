// Engine adapter trait for unified benchmarking
// 统一的引擎适配器 trait

use std::{future::Future, path::Path};

use crate::Result;

/// Calculate directory size iteratively
/// 迭代计算目录大小
pub fn dir_size(path: &Path) -> u64 {
  if !path.exists() {
    return 0;
  }
  let mut total = 0u64;
  let mut stack = vec![path.to_path_buf()];

  while let Some(cur) = stack.pop() {
    if let Ok(entries) = std::fs::read_dir(&cur) {
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

/// Unified engine adapter trait
/// 统一引擎适配器 trait
///
/// No Send bound since compio is single-threaded async runtime.
/// 无 Send 约束，因为 compio 是单线程异步运行时。
pub trait BenchEngine {
  /// Value type returned by get (zero-copy support)
  /// get 返回的值类型（支持零拷贝）
  type Val: AsRef<[u8]>;

  /// Engine name / 引擎名称
  fn name(&self) -> &str;

  /// Data directory path / 数据目录路径
  fn data_path(&self) -> &Path;

  /// Put key-value / 写入键值
  fn put(&mut self, key: &[u8], val: &[u8]) -> impl Future<Output = Result<()>>;

  /// Get value by key / 按键读取
  fn get(&mut self, key: &[u8]) -> impl Future<Output = Result<Option<Self::Val>>>;

  /// Delete key / 删除键
  fn rm(&mut self, key: &[u8]) -> impl Future<Output = Result<()>>;

  /// Sync to disk / 同步到磁盘
  fn sync(&mut self) -> impl Future<Output = Result<()>>;

  /// Reset internal stats (called after warmup)
  /// 重置内部统计（预热后调用）
  fn reset_stats(&mut self) {}

  /// Flush in-memory data to disk before read test
  /// 读测试前将内存数据刷到磁盘
  fn flush_before_read(&mut self) {}

  /// Print stats on drop (optional)
  /// 销毁时打印统计（可选）
  fn print_stats(&self) {}

  /// Get disk usage in bytes
  /// 获取磁盘使用量（字节）
  fn disk_usage(&self) -> u64 {
    dir_size(self.data_path())
  }

  /// Simulated component memory overhead (e.g. HashMap for index simulation)
  /// 模拟组件的内存开销（如用于模拟索引的 HashMap）
  fn sim_mem(&self) -> u64 {
    0
  }
}
