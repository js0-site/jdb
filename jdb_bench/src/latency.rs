// Latency histogram wrapper for HdrHistogram
// 延迟直方图包装器

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::Result;

/// Max latency value: 1 hour in nanoseconds
/// 最大延迟值：1小时（纳秒）
const MAX_LATENCY_NS: u64 = 3_600_000_000_000;

/// Significant figures for histogram precision
/// 直方图精度的有效数字
const SIGFIG: u8 = 3;

/// Latency histogram for recording operation latencies
/// 用于记录操作延迟的直方图
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
  inner: Histogram<u64>,
}

impl Default for LatencyHistogram {
  fn default() -> Self {
    Self::new().expect("Failed to create default histogram")
  }
}

impl LatencyHistogram {
  /// Create new latency histogram
  /// 创建新的延迟直方图
  pub fn new() -> Result<Self> {
    let inner = Histogram::new_with_bounds(1, MAX_LATENCY_NS, SIGFIG)?;
    Ok(Self { inner })
  }

  /// Record a latency value in nanoseconds
  /// 记录延迟值（纳秒）
  pub fn record(&mut self, latency_ns: u64) -> Result<()> {
    self.inner.record(latency_ns)?;
    Ok(())
  }

  /// Record a latency value, saturating at max
  /// 记录延迟值，超出范围时饱和
  pub fn record_saturating(&mut self, latency_ns: u64) {
    let val = latency_ns.min(MAX_LATENCY_NS);
    let _ = self.inner.record(val);
  }

  /// Get P50 latency (median)
  /// 获取 P50 延迟（中位数）
  pub fn p50(&self) -> u64 {
    self.inner.value_at_quantile(0.50)
  }

  /// Get P99 latency
  /// 获取 P99 延迟
  pub fn p99(&self) -> u64 {
    self.inner.value_at_quantile(0.99)
  }

  /// Get P999 latency
  /// 获取 P999 延迟
  pub fn p999(&self) -> u64 {
    self.inner.value_at_quantile(0.999)
  }

  /// Get mean latency
  /// 获取平均延迟
  pub fn mean(&self) -> f64 {
    self.inner.mean()
  }

  /// Get min latency
  /// 获取最小延迟
  pub fn min(&self) -> u64 {
    self.inner.min()
  }

  /// Get max latency
  /// 获取最大延迟
  pub fn max(&self) -> u64 {
    self.inner.max()
  }

  /// Get total count of recorded values
  /// 获取记录的值总数
  pub fn count(&self) -> u64 {
    self.inner.len()
  }

  /// Check if histogram is empty
  /// 检查直方图是否为空
  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }

  /// Reset histogram
  /// 重置直方图
  pub fn reset(&mut self) {
    self.inner.reset();
  }
}

/// Serializable latency stats snapshot
/// 可序列化的延迟统计快照
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LatencyStats {
  pub min: u64,
  pub max: u64,
  pub mean: f64,
  pub p50: u64,
  pub p99: u64,
  pub p999: u64,
  pub count: u64,
}

impl From<&LatencyHistogram> for LatencyStats {
  fn from(h: &LatencyHistogram) -> Self {
    Self {
      min: h.min(),
      max: h.max(),
      mean: h.mean(),
      p50: h.p50(),
      p99: h.p99(),
      p999: h.p999(),
      count: h.count(),
    }
  }
}
