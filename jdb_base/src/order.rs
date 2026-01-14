use std::cmp::Ordering;

/// Trait to define ordering policy (Ascending or Descending)
/// 定义排序策略（升序或降序）的 Trait
pub trait Order: 'static + Sized + Copy + Clone + Default {
  /// Compare two values according to the policy
  /// 根据策略比较两个值
  fn cmp<T: Ord + ?Sized>(a: &T, b: &T) -> Ordering;
}

/// Ascending order (Standard behavior)
/// 升序（标准行为）
#[derive(Debug, Clone, Copy, Default)]
pub struct Asc;
impl Order for Asc {
  #[inline(always)]
  fn cmp<T: Ord + ?Sized>(a: &T, b: &T) -> Ordering {
    a.cmp(b)
  }
}

/// Descending order (Reverse behavior)
/// 降序（反向行为）
#[derive(Debug, Clone, Copy, Default)]
pub struct Desc;
impl Order for Desc {
  #[inline(always)]
  fn cmp<T: Ord + ?Sized>(a: &T, b: &T) -> Ordering {
    b.cmp(a)
  }
}
