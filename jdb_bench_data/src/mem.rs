// Memory usage tracking via jemalloc
// 通过 jemalloc 追踪内存使用量

use tikv_jemalloc_ctl::{epoch, stats};
/// Jemalloc allocator type / Jemalloc 分配器类型
pub use tikv_jemallocator::Jemalloc;

/// Get current allocated bytes via jemalloc stats
/// 通过 jemalloc 统计获取当前分配字节数
#[inline]
pub fn process_mem() -> u64 {
  // Advance epoch to get fresh stats / 推进 epoch 获取最新统计
  epoch::advance().ok();
  stats::allocated::read().unwrap_or(0) as u64
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
