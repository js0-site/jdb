//! Range iterators with reference counting
//! 带引用计数的范围迭代器

use std::{cmp::Ordering, collections::btree_map, iter::FusedIterator};

use jdb_base::Pos;

use crate::{Kv, Mem};

/// Forward range iterator with Mem ownership
/// 带 Mem 所有权的正向范围迭代器
pub struct MemIter {
  // Drop order matters: inner borrows from _mem, so inner must be dropped first (declared first).
  // Drop 顺序很重要：inner 借用自 _mem，所以 inner 必须先被 Drop（先声明）。
  inner: btree_map::Range<'static, Box<[u8]>, Pos>,
  _mem: Mem,
  pub(crate) id: u64,
}

impl Iterator for MemIter {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    // Map (K, V) -> (K, V) (owned)
    // 映射 (K, V) -> (K, V) (所有权)
    self.inner.next().map(|(k, v)| (k.clone(), *v))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.inner.size_hint()
  }
}

impl DoubleEndedIterator for MemIter {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.inner.next_back().map(|(k, v)| (k.clone(), *v))
  }
}

impl FusedIterator for MemIter {}
impl ExactSizeIterator for MemIter {}

impl Eq for MemIter {}

impl PartialEq for MemIter {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl PartialOrd for MemIter {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemIter {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.id.cmp(&other.id)
  }
}

/// Reverse range iterator with Mem ownership
/// 带 Mem 所有权的反向范围迭代器
pub struct MemRevIter(MemIter);

impl Iterator for MemRevIter {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.0.next_back()
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.0.size_hint()
  }
}

impl FusedIterator for MemRevIter {}
impl ExactSizeIterator for MemRevIter {}

impl PartialEq for MemRevIter {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.0.id == other.0.id
  }
}

impl Eq for MemRevIter {}

impl PartialOrd for MemRevIter {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemRevIter {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.0.id.cmp(&other.0.id)
  }
}
