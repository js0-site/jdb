//! Sort order types for merge streams
//! 合并流的排序类型

use std::cmp::Ordering;

/// Sort order trait
/// 排序顺序 trait
pub trait Order {
  fn cmp(a: &[u8], b: &[u8]) -> Ordering;
}

/// Ascending order
/// 升序
pub struct Asc;

impl Order for Asc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
  }
}

/// Descending order
/// 降序
pub struct Desc;

impl Order for Desc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
  }
}
