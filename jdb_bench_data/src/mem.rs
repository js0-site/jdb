// Memory usage tracking
// 内存使用量追踪

/// Get current process memory usage in bytes
/// 获取当前进程内存使用量（字节）
#[inline]
pub fn process_mem() -> u64 {
  memory_stats::memory_stats()
    .map(|s| s.physical_mem as u64)
    .unwrap_or(0)
}

/// Memory baseline for tracking database memory usage
/// 内存基准点，用于追踪数据库内存使用量
#[derive(Debug, Clone, Copy)]
pub struct MemBaseline {
  baseline: u64,
}

impl MemBaseline {
  /// Record current memory as baseline (call after data preload, before db open)
  /// 记录当前内存为基准点（在数据预加载后、数据库打开前调用）
  #[inline]
  pub fn record() -> Self {
    Self {
      baseline: process_mem(),
    }
  }

  /// Get database memory usage (current - baseline)
  /// 获取数据库内存使用量（当前 - 基准点）
  #[inline]
  pub fn db_mem(&self) -> u64 {
    process_mem().saturating_sub(self.baseline)
  }

  /// Get baseline value
  /// 获取基准点值
  #[inline]
  pub fn baseline(&self) -> u64 {
    self.baseline
  }
}
