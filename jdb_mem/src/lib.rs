#![cfg_attr(docsrs, feature(doc_cfg))]

//! Kv - Key-Value abstractions and Table trait
//! Kv - 键值对抽象与表接口 Trait

use std::{collections::btree_map, ops::Bound};

pub use jdb_base::Pos;

mod flush;
mod iter;
pub mod mem;
mod mems;
mod merge;

pub use iter::{MemIter, MemRevIter};
pub use jdb_base::Kv;
pub use mem::{Mem, MemInner};
pub use mems::{DEFAULT_MEM_SIZE, Mems};
pub use merge::{MergeIter, MergeRevIter};

/// Forward iterator for Mem
/// Mem 正向迭代器
pub struct MemTableIter<'a>(btree_map::Range<'a, Box<[u8]>, Pos>);

impl Iterator for MemTableIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.0.next().map(|(k, v)| (k.clone(), *v))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.0.size_hint()
  }
}

/// Reverse iterator for Mem
/// Mem 反向迭代器
pub struct MemTableRevIter<'a>(btree_map::Range<'a, Box<[u8]>, Pos>);

impl Iterator for MemTableRevIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.0.next_back().map(|(k, v)| (k.clone(), *v))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.0.size_hint()
  }
}

impl jdb_base::Mem for MemInner {
  type Iter<'a> = MemTableIter<'a>;
  type RevIter<'a> = MemTableRevIter<'a>;

  #[inline]
  fn get(&self, key: &[u8]) -> Option<Pos> {
    self.get(key)
  }

  #[inline]
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter<'_> {
    MemTableIter(self.data.range::<[u8], _>((start, end)))
  }

  #[inline]
  fn rev_range(&self, end: Bound<&[u8]>, start: Bound<&[u8]>) -> Self::RevIter<'_> {
    MemTableRevIter(self.data.range::<[u8], _>((start, end)))
  }

  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.put(key, pos)
  }

  #[inline]
  fn rm(&mut self, key: impl Into<Box<[u8]>>) {
    self.rm(key)
  }
}

impl<F: jdb_base::sst::Flush, N: jdb_base::sst::OnFlush> jdb_base::Mem for Mems<F, N> {
  type Iter<'a> = MergeIter<'a>;
  type RevIter<'a> = MergeRevIter<'a>;

  #[inline]
  fn get(&self, key: &[u8]) -> Option<Pos> {
    Mems::get(self, key)
  }

  #[inline]
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter<'_> {
    self.merge_range(start, end)
  }

  #[inline]
  fn rev_range(&self, end: Bound<&[u8]>, start: Bound<&[u8]>) -> Self::RevIter<'_> {
    self.merge_rev_range(start, end)
  }

  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    Mems::put(self, key, pos);
  }

  #[inline]
  fn rm(&mut self, key: impl Into<Box<[u8]>>) {
    Mems::rm(self, key);
  }
}
