//! Order - Merge order traits and heap item
//! 排序 - 合并排序 trait 和堆元素

use std::{cmp::Ordering, marker::PhantomData};

use crate::Pos;

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
    // To pop smaller keys first, we reverse the comparison (return Greater if a < b).
    // BinaryHeap 是最大堆（弹出最大值）。
    // 为了先弹出较小的键，我们要反转比较结果（如果 a < b 则返回 Greater）。
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
    // Standard comparison. Larger keys pop first.
    // 标准比较。较大的键先弹出。
    a.cmp(b)
  }
}

/// Heap item for merge sort
/// 合并排序用的堆元素
#[derive(Debug)]
pub(super) struct Item<O> {
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
      // Same key: we want the one with LOWER src_idx (newer) to pop first.
      // Since it's a Max-Heap, the one popping first must be "Greater".
      // comparison: 1.cmp(0) -> Greater. So if we use other.idx.cmp(self.idx),
      // then item(idx=0) is "Greater" than item(idx=1).
      // 相同键：我们需要 src_idx 较小（较新）的先弹出。
      // 因为是最大堆，先弹出的必须判定为 "Greater"。
      // 比较逻辑：1.cmp(0) -> Greater。所以使用 other.idx.cmp(self.idx) 时，
      // item(idx=0) 会比 item(idx=1) "更大"，从而先弹出。
      Ordering::Equal => other.src_idx.cmp(&self.src_idx),
      ord => ord,
    }
  }
}
