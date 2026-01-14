use std::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

/// Helper to compare bounds for pruning
///
/// start_bound of item vs end_bound of query
/// optimized for sorted items
///
/// 边界比较辅助函数，用于剪枝
///
/// 项的起始边界 vs 查询的结束边界
/// 针对有序项进行优化
#[inline(always)]
/// Helper to compare bounds for pruning
///
/// start_bound of item vs end_bound of query
/// optimized for sorted items
///
/// 边界比较辅助函数，用于剪枝
///
/// 项的起始边界 vs 查询的结束边界
/// 针对有序项进行优化
fn is_after<B, Q1, Q2>(item_start: Bound<&Q1>, query_end: Bound<&Q2>) -> bool
where
  B: PartialOrd + ?Sized,
  Q1: Borrow<B> + ?Sized,
  Q2: Borrow<B> + ?Sized,
{
  use Bound::*;
  match (item_start, query_end) {
    (Included(s), Included(e)) => s.borrow() > e.borrow(),
    (Included(s), Excluded(e)) => s.borrow() >= e.borrow(),
    (Excluded(s), Included(e)) => s.borrow() >= e.borrow(),
    (Excluded(s), Excluded(e)) => s.borrow() >= e.borrow(),
    (Unbounded, _) => false,
    (_, Unbounded) => false,
  }
}

/// Helper to compare bounds for binary search start
///
/// end_bound of item vs start_bound of query
/// check if item is purely BEFORE query
///
/// 边界比较辅助函数，用于二分查找起始位置
///
/// 项的结束边界 vs 查询的起始边界
/// 检查项是否完全在查询之前
#[inline(always)]
fn is_before<B, Q1, Q2>(item_end: Bound<&Q1>, query_start: Bound<&Q2>) -> bool
where
  B: PartialOrd + ?Sized,
  Q1: Borrow<B> + ?Sized,
  Q2: Borrow<B> + ?Sized,
{
  use Bound::*;
  match (item_end, query_start) {
    (Included(e), Included(s)) => e.borrow() < s.borrow(),
    (Included(e), Excluded(s)) => e.borrow() <= s.borrow(),
    (Excluded(e), Included(s)) => e.borrow() <= s.borrow(),
    (Excluded(e), Excluded(s)) => e.borrow() <= s.borrow(),
    (Unbounded, _) => false,
    (_, Unbounded) => false,
  }
}

/// Helper function to find overlapping items in a sorted slice effectively
///
/// **Scenario**:
/// This function is optimized for **disjoint** ranges sorted by their start positions (e.g., SST metadata).
/// Under this condition, the end positions are monotonic, ensuring the correctness of binary search.
///
/// 在有序切片中高效查找重叠项的辅助函数
///
/// **适用场景**：
/// 本函数针对按起始位置排序且**互不重叠**的区间（如 SST 元数据）进行了优化。
/// 在此条件下，结束位置是单调的，从而保证了二分查找的正确性。
#[inline]
pub fn overlap_for_sorted<T, B, Q1, Q2, R1, R2>(range: R2, slice: &[T]) -> impl Iterator<Item = &T>
where
  B: PartialOrd + ?Sized,
  T: Borrow<R1>,
  Q1: Borrow<B> + ?Sized,
  Q2: Borrow<B> + ?Sized,
  R1: RangeBounds<Q1>,
  R2: RangeBounds<Q2>,
{
  // Cache range bounds to avoid repeated calls
  let range_start = range.start_bound();
  let range_end = range.end_bound();
  // 缓存范围边界，避免重复调用

  // 1. Determine start index: find first item that is NOT before query
  let start_idx = slice.partition_point(|item| {
    let r1: &R1 = <T as Borrow<R1>>::borrow(item);
    is_before(r1.end_bound(), range_start)
  });
  // 1. 确定起始索引：找到第一个不在查询之前的项

  // Optimization: Early exit if all items are before the query
  if start_idx == slice.len() {
    return slice[..0].iter();
  }

  // Optimization: Early exit if the first candidate is already after the query
  // Since items are sorted by start_bound, if the first one is after, all are after.
  // This transforms the "gap miss" case from O(log N) to O(1).
  //
  // 优化：如果所有项都在查询之前，则提前退出
  // 优化：如果第一个候选项已经在查询之后，则提前退出
  // 由于项按 start_bound 排序，如果第一个在之后，则后续所有项都在之后。
  // 这将“间隙未命中”的情况从 O(log N) 优化为 O(1)。
  {
    let item = unsafe { slice.get_unchecked(start_idx) };
    let r1: &R1 = <T as Borrow<R1>>::borrow(item);
    if is_after(r1.start_bound(), range_end) {
      return slice[..0].iter();
    }
  }

  // 2. Determine end index: find first item that is AFTER query (pruning tail)
  // partition_point guarantees start_idx is in 0..=slice.len()
  let relevant_slice = unsafe { slice.get_unchecked(start_idx..) };
  let count = relevant_slice.partition_point(|item| {
    let r1: &R1 = <T as Borrow<R1>>::borrow(item);
    !is_after(r1.start_bound(), range_end)
  });
  // 2. 确定结束索引：找到第一个在查询之后的项（尾部剪枝）
  // partition_point 保证 start_idx 在 0..=slice.len() 范围内

  // 3. Return the candidate slice as iterator
  //
  // NOTE: The final .filter() call is removed.
  // This is mathematically valid because the two partition_point calls strictly bound the range:
  // 1. `start_idx` ensures: item.end >= query.start (Not entirely before)
  // 2. `count` ensures: item.start <= query.end (Not entirely after)
  //
  // Combined, these imply overlap: (item.start <= query.end) AND (query.start <= item.end).
  //
  // **Verification**:
  // This logic works correctly because the input `slice` contains disjoint, sorted ranges,
  // which makes the end bounds monotonic, satisfying `partition_point` requirements.
  // Verified by `tests/test_filter_necessity.rs`.
  //
  // 3. 返回候选切片的迭代器
  //
  // 注意：移除了最终的 .filter() 调用。
  // 这在数学上是有效的，因为两次 partition_point 调用严格限制了范围：
  // 1. `start_idx` 确保：item.end >= query.start（不完全在之前）
  // 2. `count` 确保：item.start <= query.end（不完全在之后）
  //
  // 两者结合隐含了重叠条件：(item.start <= query.end) AND (query.start <= item.end)。
  //
  // **验证**：
  // 该逻辑正确的前提是输入 `slice`包含互不重叠且有序的区间，
  // 这使得结束边界单调，满足 `partition_point` 的要求。
  // 已通过 `tests/test_filter_necessity.rs` 验证。
  let candidate_slice = unsafe { relevant_slice.get_unchecked(..count) };
  candidate_slice.iter()
}
