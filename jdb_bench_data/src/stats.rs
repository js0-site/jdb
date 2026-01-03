// Dataset statistics module
// 数据集统计模块

use serde::Serialize;

/// Size distribution bucket / 大小分布桶
#[derive(Debug, Clone, Serialize)]
pub struct SizeBucket {
  /// Bucket label (e.g., "<1KB", "1-4KB") / 桶标签
  pub label: String,
  /// Number of items in this bucket / 此桶中的条目数
  pub count: usize,
  /// Percentage of total items / 占总条目的百分比
  pub percent: f64,
  /// Total size of items in this bucket (bytes) / 此桶中条目的总大小（字节）
  pub total_size_bytes: u64,
  /// Percentage of total size / 占总大小的百分比
  pub size_percent: f64,
}

/// Dataset statistics / 数据集统计
#[derive(Debug, Clone, Serialize)]
pub struct DatasetStats {
  /// Total data size in bytes / 数据总大小（字节）
  pub total_size_bytes: u64,
  /// Number of unique items / 唯一条目数
  pub item_count: usize,
  /// Average item size in bytes / 平均条目大小（字节）
  pub avg_item_size: usize,
  /// Min item size in bytes / 最小条目大小（字节）
  pub min_item_size: usize,
  /// Max item size in bytes / 最大条目大小（字节）
  pub max_item_size: usize,
  /// Size distribution / 大小分布
  pub size_distribution: Vec<SizeBucket>,
}

impl DatasetStats {
  /// Calculate stats from key-value pairs (value size only)
  /// 从键值对计算统计（仅统计 value 大小）
  pub fn from_kv<K: AsRef<[u8]>, V: AsRef<[u8]>>(data: &[(K, V)]) -> Self {
    // Only count value size for distribution matching
    // 仅统计 value 大小以匹配分布
    let sizes: Vec<usize> = data.iter().map(|(_, v)| v.as_ref().len()).collect();

    let total: u64 = sizes.iter().map(|&s| s as u64).sum();
    let min = sizes.iter().copied().min().unwrap_or(0);
    let max = sizes.iter().copied().max().unwrap_or(0);
    let avg = if data.is_empty() {
      0
    } else {
      total as usize / data.len()
    };

    Self {
      total_size_bytes: total,
      item_count: data.len(),
      avg_item_size: avg,
      min_item_size: min,
      max_item_size: max,
      size_distribution: calc_size_distribution(&sizes, data.len()),
    }
  }

  /// Calculate stats from sizes only / 仅从大小计算统计
  pub fn from_sizes(sizes: &[usize]) -> Self {
    let total: u64 = sizes.iter().map(|&s| s as u64).sum();
    let min = sizes.iter().copied().min().unwrap_or(0);
    let max = sizes.iter().copied().max().unwrap_or(0);
    let avg = if sizes.is_empty() {
      0
    } else {
      total as usize / sizes.len()
    };

    Self {
      total_size_bytes: total,
      item_count: sizes.len(),
      avg_item_size: avg,
      min_item_size: min,
      max_item_size: max,
      size_distribution: calc_size_distribution(sizes, sizes.len()),
    }
  }
}

/// Facebook USR/APP/VAR pool size tiers / Facebook USR/APP/VAR 池大小层级
/// Reference: FAST'20 Facebook RocksDB workload analysis
/// 参考：FAST'20 Facebook RocksDB 工作负载分析
/// Note: boundaries match workload.rs SizeTier definitions
/// 注意：边界与 workload.rs 的 SizeTier 定义一致
const SIZE_TIERS: &[(&str, usize, usize)] = &[
  ("16-100B", 16, 100),           // Tiny Metadata
  ("100B-1KB", 100, 1024),        // Small Structs
  ("1-10KB", 1024, 10240),        // Medium Content
  ("10-100KB", 10240, 102400),    // Large Objects
  ("100KB-1MB", 102400, 1048576), // Huge Blobs
];

/// Calculate size distribution using fixed Facebook tiers
/// 使用固定的 Facebook 层级计算大小分布
fn calc_size_distribution(sizes: &[usize], total_count: usize) -> Vec<SizeBucket> {
  if sizes.is_empty() {
    return Vec::new();
  }

  let total_size: u64 = sizes.iter().map(|&s| s as u64).sum();
  let mut buckets = Vec::with_capacity(SIZE_TIERS.len());

  for &(label, min, max) in SIZE_TIERS {
    let mut count = 0usize;
    let mut bucket_size = 0u64;

    for &size in sizes {
      // [min, max) range, except last tier includes max
      // [min, max) 范围，最后一层包含 max
      let in_range = if max == 1048576 {
        size >= min && size <= max
      } else {
        size >= min && size < max
      };

      if in_range {
        count += 1;
        bucket_size += size as u64;
      }
    }

    buckets.push(SizeBucket {
      label: label.to_string(),
      count,
      percent: if total_count > 0 {
        count as f64 / total_count as f64 * 100.0
      } else {
        0.0
      },
      total_size_bytes: bucket_size,
      size_percent: if total_size > 0 {
        bucket_size as f64 / total_size as f64 * 100.0
      } else {
        0.0
      },
    });
  }

  buckets
}

/// Format bytes to human readable / 格式化字节为可读形式
pub fn fmt_size(bytes: u64) -> String {
  if bytes >= 1024 * 1024 {
    format!("{:.1}MB", bytes as f64 / 1024.0 / 1024.0)
  } else if bytes >= 1024 {
    format!("{:.1}KB", bytes as f64 / 1024.0)
  } else {
    format!("{bytes}B")
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_stats_from_sizes() {
    let sizes = vec![100, 200, 300, 400, 500];
    let stats = DatasetStats::from_sizes(&sizes);
    assert_eq!(stats.item_count, 5);
    assert_eq!(stats.total_size_bytes, 1500);
    assert_eq!(stats.avg_item_size, 300);
    assert_eq!(stats.min_item_size, 100);
    assert_eq!(stats.max_item_size, 500);
  }

  #[test]
  fn test_fmt_size() {
    assert_eq!(fmt_size(100), "100B");
    assert_eq!(fmt_size(1024), "1.0KB");
    assert_eq!(fmt_size(1536), "1.5KB");
    assert_eq!(fmt_size(1048576), "1.0MB");
  }
}
