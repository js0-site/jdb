//! Meta - Table metadata trait for compaction
//! 表元数据 trait，用于 compaction 决策

use std::ops::Bound;

/// Table metadata trait for compaction decisions
/// 表元数据 trait，用于 compaction 决策
pub trait Meta {
  /// Table unique id (monotonically increasing)
  /// 表唯一 ID（单调递增）
  fn id(&self) -> u64;

  /// Minimum key in table
  /// 表中最小键
  fn min_key(&self) -> &[u8];

  /// Maximum key in table
  /// 表中最大键
  fn max_key(&self) -> &[u8];

  /// Table size in bytes
  /// 表大小（字节）
  fn size(&self) -> u64;

  /// Entry count
  /// 条目数量
  fn count(&self) -> u64;

  /// Delete/tombstone count for compaction priority
  /// 删除标记数量，用于压缩优先级
  fn rm_count(&self) -> u64;

  /// Compensated size = size + rm_count * avg_entry_size
  /// 补偿大小 = 大小 + 删除数 * 平均条目大小
  #[inline]
  fn compensated_size(&self) -> u64 {
    let avg = if self.count() > 0 {
      self.size() / self.count()
    } else {
      64 // default avg entry size / 默认平均条目大小
    };
    self
      .size()
      .saturating_add(self.rm_count().saturating_mul(avg))
  }

  /// Check if key is in range [min_key, max_key]
  /// 检查键是否在范围内
  #[inline]
  fn contains(&self, key: &[u8]) -> bool {
    let min = self.min_key();
    !min.is_empty() && key >= min && key <= self.max_key()
  }

  /// Check if range overlaps with table
  /// 检查范围是否与表重叠
  #[inline]
  fn overlaps(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let min = self.min_key();
    let max = self.max_key();

    // Empty table has no overlap
    // 空表无重叠
    if min.is_empty() {
      return false;
    }

    // Check start > max_key
    // 检查 start > max_key
    let start_past = match start {
      Bound::Unbounded => false,
      Bound::Included(k) => k > max,
      Bound::Excluded(k) => k >= max,
    };
    if start_past {
      return false;
    }

    // Check end < min_key
    // 检查 end < min_key
    match end {
      Bound::Unbounded => true,
      Bound::Included(k) => k >= min,
      Bound::Excluded(k) => k > min,
    }
  }
}
