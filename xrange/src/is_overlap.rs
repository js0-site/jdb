use std::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

/// Check if two ranges overlap
///
/// 检查两个范围是否重叠
#[inline]
#[must_use]
pub fn is_overlap<T, Q, R1, R2>(r1: &R1, r2: &R2) -> bool
where
  T: PartialOrd + ?Sized,
  Q: Borrow<T> + ?Sized,
  R1: RangeBounds<Q>,
  R2: RangeBounds<Q>,
{
  use Bound::*;

  // Cache bounds to avoid repeated calls
  let r1_start = r1.start_bound();
  let r1_end = r1.end_bound();
  let r2_start = r2.start_bound();
  let r2_end = r2.end_bound();
  // 缓存边界值，避免重复调用

  // Overlap condition: r1.start <= r2.end AND r2.start <= r1.end
  // 重叠条件：r1.start <= r2.end 且 r2.start <= r1.end

  // Check r1.start <= r2.end
  let start_ok = match (r1_start, r2_end) {
    (Included(s1), Included(e2)) => s1.borrow() <= e2.borrow(),
    (Included(s1), Excluded(e2)) => s1.borrow() < e2.borrow(),
    (Excluded(s1), Included(e2)) => s1.borrow() < e2.borrow(),
    (Excluded(s1), Excluded(e2)) => s1.borrow() < e2.borrow(),
    (_, Unbounded) => true,
    (Unbounded, _) => true,
  };

  if !start_ok {
    return false;
  }

  // Check r2.start <= r1.end
  match (r2_start, r1_end) {
    (Included(s2), Included(e1)) => s2.borrow() <= e1.borrow(),
    (Included(s2), Excluded(e1)) => s2.borrow() < e1.borrow(),
    (Excluded(s2), Included(e1)) => s2.borrow() < e1.borrow(),
    (Excluded(s2), Excluded(e1)) => s2.borrow() < e1.borrow(),
    (_, Unbounded) => true,
    (Unbounded, _) => true,
  }
}
