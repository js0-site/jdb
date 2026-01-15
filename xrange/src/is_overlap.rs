use std::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

/// Check if a range overlaps with an item
///
/// Returns true if the query range and item range have any intersection.
///
/// 检查查询范围与项是否重叠
///
/// 如果查询范围与项范围有交集则返回 true。
#[inline]
#[must_use]
pub fn is_overlap<T, Q1, Q2, R1, R2>(range: &R1, item: &R2) -> bool
where
  T: PartialOrd + ?Sized,
  Q1: Borrow<T> + ?Sized,
  Q2: Borrow<T> + ?Sized,
  R1: RangeBounds<Q1>,
  R2: RangeBounds<Q2>,
{

  // Cache bounds to avoid repeated calls
  // 缓存边界值，避免重复调用
  let range_start = range.start_bound();
  let range_end = range.end_bound();
  let item_start = item.start_bound();
  let item_end = item.end_bound();

  // Overlap condition: range.start <= item.end AND item.start <= range.end
  // 重叠条件：range.start <= item.end 且 item.start <= range.end

  // Check range.start <= item.end (item is not entirely before range)
  // 检查 range.start <= item.end（项不完全在范围之前）
  let not_before = match (range_start, item_end) {
    (Bound::Included(rs), Bound::Included(ie)) => rs.borrow() <= ie.borrow(),
    (Bound::Included(rs), Bound::Excluded(ie)) => rs.borrow() < ie.borrow(),
    (Bound::Excluded(rs), Bound::Included(ie)) => rs.borrow() < ie.borrow(),
    (Bound::Excluded(rs), Bound::Excluded(ie)) => rs.borrow() < ie.borrow(),
    (_, Bound::Unbounded) => true,
    (Bound::Unbounded, _) => true,
  };

  if !not_before {
    return false;
  }

  // Check item.start <= range.end (item is not entirely after range)
  // 检查 item.start <= range.end（项不完全在范围之后）
  match (item_start, range_end) {
    (Bound::Included(is), Bound::Included(re)) => is.borrow() <= re.borrow(),
    (Bound::Included(is), Bound::Excluded(re)) => is.borrow() < re.borrow(),
    (Bound::Excluded(is), Bound::Included(re)) => is.borrow() < re.borrow(),
    (Bound::Excluded(is), Bound::Excluded(re)) => is.borrow() < re.borrow(),
    (_, Bound::Unbounded) => true,
    (Bound::Unbounded, _) => true,
  }
}
