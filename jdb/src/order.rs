//! Order - Merge order traits and heap item
//! 排序 - 合并排序 trait 和堆元素

use std::{cmp::Ordering, marker::PhantomData};

use jdb_base::Pos;

/// Order trait for merge
/// 合并排序 trait
pub trait Order {
  /// Compare two keys
  /// 比较两个键
  fn cmp(a: &[u8], b: &[u8]) -> Ordering;
}

/// Ascending order (simulated min-heap behavior in max-heap)
/// 升序（在最大堆中模拟最小堆行为）
#[derive(Debug, Clone, Copy, Default)]
pub struct Asc;

impl Order for Asc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    // BinaryHeap is a max-heap (pops greatest).
    // To pop smaller keys first, we reverse the comparison.
    // BinaryHeap 是最大堆（弹出最大值）。
    // 为了先弹出较小的键，我们要反转比较结果。
    b.cmp(a)
  }
}

/// Descending order
/// 降序
#[derive(Debug, Clone, Copy, Default)]
pub struct Desc;

impl Order for Desc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
  }
}

/// Heap item for merge sort
/// 合并排序用的堆元素
pub(crate) struct Item<O> {
  pub key: Box<[u8]>,
  pub pos: Pos,
  /// Source index (0 = Memtable/Newest, Higher = Older SSTs)
  /// 源索引（0 = 内存表/最新，越高越旧）
  pub src_idx: usize,
  _o: PhantomData<O>,
}

impl<O> Item<O> {
  #[inline]
  pub fn new(key: Box<[u8]>, pos: Pos, src_idx: usize) -> Self {
    Self {
      key,
      pos,
      src_idx,
      _o: PhantomData,
    }
  }
}

impl<O> Eq for Item<O> {}

impl<O> PartialEq for Item<O> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.src_idx == other.src_idx
  }
}

impl<O: Order> PartialOrd for Item<O> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<O: Order> Ord for Item<O> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    match O::cmp(&self.key, &other.key) {
      // Same key: lower src_idx (newer) pops first
      // 相同键：src_idx 较小（较新）的先弹出
      Ordering::Equal => other.src_idx.cmp(&self.src_idx),
      ord => ord,
    }
  }
}
