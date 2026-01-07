//! Order - Merge order traits
//! 排序 - 合并排序 trait

use std::cmp::Ordering;

/// Order trait for merge sort
/// 合并排序 trait
pub trait Order {
  /// Compare two keys
  /// 比较两个键
  fn cmp(a: &[u8], b: &[u8]) -> Ordering;
}

/// Ascending order
/// 升序
#[derive(Debug, Clone, Copy, Default)]
pub struct Asc;

impl Order for Asc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
  }
}

/// Descending order
/// 降序
#[derive(Debug, Clone, Copy, Default)]
pub struct Desc;

impl Order for Desc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
  }
}
