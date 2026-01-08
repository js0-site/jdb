//! Range iterators with reference counting
//! 带引用计数的范围迭代器

use std::{cmp::Ordering, collections::btree_map, iter::FusedIterator, marker::PhantomData};

use jdb_base::Pos;

use crate::{Kv, Mem};

/// Forward range iterator with Mem ownership
/// 带 Mem 所有权的正向范围迭代器
pub struct MemIter<'a> {
  // Drop order matters: inner borrows from _mem, so inner must be dropped first (declared first).
  // Drop 顺序很重要：inner 借用自 _mem，所以 inner 必须先被 Drop（先声明）。
  inner: btree_map::Range<'static, Box<[u8]>, Pos>,
  _mem: Mem,
  _marker: PhantomData<&'a [u8]>,
  pub(crate) id: u64,
}

impl Iterator for MemIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|(k, v)| (k.clone(), *v))
  }
}

impl DoubleEndedIterator for MemIter<'_> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.inner.next_back().map(|(k, v)| (k.clone(), *v))
  }
}

impl FusedIterator for MemIter<'_> {}

impl Eq for MemIter<'_> {}

impl PartialEq for MemIter<'_> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl PartialOrd for MemIter<'_> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemIter<'_> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.id.cmp(&other.id)
  }
}

/// Reverse range iterator with Mem ownership
/// 带 Mem 所有权的反向范围迭代器
pub struct MemRevIter<'a>(MemIter<'a>);

impl Iterator for MemRevIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.0.next_back()
  }
}

impl FusedIterator for MemRevIter<'_> {}

impl PartialEq for MemRevIter<'_> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.0.id == other.0.id
  }
}

impl Eq for MemRevIter<'_> {}

impl PartialOrd for MemRevIter<'_> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemRevIter<'_> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.0.id.cmp(&other.0.id)
  }
}
