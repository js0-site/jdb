#![cfg_attr(docsrs, feature(doc_cfg))]

//! Kv - Key-Value abstractions and Table trait
//! Kv - 键值对抽象与表接口 Trait

use std::ops::Bound;

pub use jdb_base::Pos;

mod handle;
mod iter;
mod mem;
mod mems;
mod merge;

pub use handle::Handle;
pub use jdb_base::table::{
  Kv,
  mem::{Order, Table, TableMut},
};
pub use mem::Mem;
pub use mems::Mems;
pub use merge::{MergeIter, MergeRevIter};
pub use iter::{MemIter, MemRevIter};

/// Merge iterator enum for both directions
/// 双向归并迭代器枚举
pub enum MergeIterEnum {
  Asc(MergeIter),
  Desc(merge::MergeRevIter),
}

impl Iterator for MergeIterEnum {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self {
      Self::Asc(iter) => iter.next(),
      Self::Desc(iter) => iter.next(),
    }
  }
}

impl Table for Mems {
  type Iter<'a> = MergeIterEnum;

  #[inline]
  fn get(&self, key: &[u8]) -> Option<Pos> {
    Mems::get(self, key)
  }

  #[inline]
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>, order: Order) -> Self::Iter<'_> {
    match order {
      Order::Asc => MergeIterEnum::Asc(self.merge_range(start, end)),
      Order::Desc => MergeIterEnum::Desc(self.merge_rev_range(start, end)),
    }
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

impl Mems {
  /// Create merge iterator over all memtables
  /// 创建所有内存表的归并迭代器
  pub fn merge_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeIter {
    let handles = self.all_handles();
    MergeIter::new(handles, start, end)
  }

  /// Create reverse merge iterator
  /// 创建反向归并迭代器
  pub fn merge_rev_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> merge::MergeRevIter {
    let handles = self.all_handles();
    merge::MergeRevIter::new(handles, start, end)
  }
}
