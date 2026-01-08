#![cfg_attr(docsrs, feature(doc_cfg))]

//! Kv - Key-Value abstractions and Table trait
//! Kv - 键值对抽象与表接口 Trait

use std::{collections::btree_map, ops::Bound};

pub use jdb_base::Pos;

mod iter;
pub mod mem;
mod mems;
mod merge;

pub use iter::{MemIter, MemRevIter};
pub use jdb_base::table::{
  Kv,
  mem::{Table, TableMut},
};
pub use mem::{Mem, MemInner};
pub use mems::Mems;
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
}

impl Table for MemInner {
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
}

impl Table for Mems {
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
}

impl TableMut for Mems {
  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.active_mut().put(key, pos);
  }

  #[inline]
  fn rm(&mut self, key: impl Into<Box<[u8]>>) {
    self.active_mut().rm(key);
  }
}
