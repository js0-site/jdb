// Benchmark metrics types
// 基准测试指标类型

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::LatencyStats;

/// Benchmark metrics for a single test run
/// 单次测试运行的基准测试指标
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchMetrics {
  /// Total operations count / 总操作数
  pub ops_count: u64,
  /// Total duration in nanoseconds / 总耗时（纳秒）
  pub duration_ns: u64,
  /// Throughput (ops/sec) / 吞吐量
  pub throughput: f64,
  /// Latency statistics / 延迟统计
  pub latency: LatencyStats,
  /// Disk usage in bytes / 磁盘使用量（字节）
  pub disk_bytes: u64,
  /// Memory usage in bytes / 内存使用量（字节）
  pub memory_bytes: u64,
}

impl BenchMetrics {
  /// Create new metrics from raw data
  /// 从原始数据创建指标
  pub fn new(
    ops_count: u64,
    duration: Duration,
    latency: LatencyStats,
    disk_bytes: u64,
    memory_bytes: u64,
  ) -> Self {
    let duration_ns = duration.as_nanos() as u64;
    let throughput = if duration_ns > 0 {
      ops_count as f64 / duration.as_secs_f64()
    } else {
      0.0
    };

    Self {
      ops_count,
      duration_ns,
      throughput,
      latency,
      disk_bytes,
      memory_bytes,
    }
  }

  /// Get duration as Duration type
  /// 获取 Duration 类型的耗时
  pub fn duration(&self) -> Duration {
    Duration::from_nanos(self.duration_ns)
  }
}

/// Space efficiency metrics
/// 空间效率指标
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpaceMetrics {
  /// Logical data size in bytes / 逻辑数据大小（字节）
  pub logical_bytes: u64,
  /// Actual disk usage in bytes / 实际磁盘使用（字节）
  pub physical_bytes: u64,
  /// Space amplification ratio / 空间放大比
  pub amplification: f64,
  /// Header overhead per slot in bytes / 每槽位头部开销（字节）
  pub header_overhead: u64,
  /// Fragmentation ratio (0.0-1.0) / 碎片率
  pub fragmentation: f64,
  /// Free map size in bytes / 空闲位图大小（字节）
  pub free_map_bytes: u64,
  /// Heat tracker size in bytes / 热度追踪器大小（字节）
  pub heat_tracker_bytes: u64,
}

impl SpaceMetrics {
  /// Create new space metrics
  /// 创建新的空间指标
  pub fn new(
    logical_bytes: u64,
    physical_bytes: u64,
    header_overhead: u64,
    free_slots: u64,
    total_slots: u64,
    free_map_bytes: u64,
    heat_tracker_bytes: u64,
  ) -> Self {
    let amplification = if logical_bytes > 0 {
      physical_bytes as f64 / logical_bytes as f64
    } else {
      1.0
    };

    let fragmentation = if total_slots > 0 {
      free_slots as f64 / total_slots as f64
    } else {
      0.0
    };

    Self {
      logical_bytes,
      physical_bytes,
      amplification,
      header_overhead,
      fragmentation,
      free_map_bytes,
      heat_tracker_bytes,
    }
  }
}
