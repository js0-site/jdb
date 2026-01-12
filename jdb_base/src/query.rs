use core::ops::{Bound, RangeBounds};
use std::borrow::Borrow;

/// Helper to map any RangeBounds to slice bounds
/// 将任何实现了 RangeBounds 的类型映射到切片边界的辅助函数
#[inline]
pub fn start_end<'a, Q: ?Sized + Borrow<[u8]> + 'a>(
  range: &'a impl RangeBounds<Q>,
) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
  let s = match range.start_bound() {
    Bound::Included(b) => Bound::Included(b.borrow()),
    Bound::Excluded(b) => Bound::Excluded(b.borrow()),
    Bound::Unbounded => Bound::Unbounded,
  };
  let e = match range.end_bound() {
    Bound::Included(b) => Bound::Included(b.borrow()),
    Bound::Excluded(b) => Bound::Excluded(b.borrow()),
    Bound::Unbounded => Bound::Unbounded,
  };
  (s, e)
}
