#![cfg_attr(docsrs, feature(doc_cfg))]

//! Kv - Key-Value abstractions and Table trait
//! Kv - 键值对抽象与表接口 Trait

use std::{collections::btree_map, iter::FusedIterator, ops::Bound};

use jdb_base::{Kv, Pos};

mod error;
mod flush;
pub mod mem;
mod mems;
mod merge;

pub use error::FlushErr;
pub use mem::{Mem, MemInner};
pub use mems::{DEFAULT_MEM_SIZE, Mems};
pub use merge::{MergeIter, MergeRevIter};

/// Key type alias (Boxed slice for immutability and compactness)
/// 键类型别名（Box 切片，用于不可变性和紧凑性）
pub type Key = Box<[u8]>;

/// Macro to define mem table iterator
/// 定义内存表迭代器的宏
macro_rules! def_mem_iter {
  ($name:ident, $next_fn:ident) => {
    impl Iterator for $name<'_> {
      type Item = Kv;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.0.$next_fn().map(|(k, v)| (k.clone(), *v))
      }

      #[inline]
      fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
      }
    }

    impl ExactSizeIterator for $name<'_> {}
    impl FusedIterator for $name<'_> {}
  };
}

/// Forward iterator for Mem
/// Mem 正向迭代器
pub struct MemTableIter<'a>(btree_map::Range<'a, Key, Pos>);
def_mem_iter!(MemTableIter, next);

/// Reverse iterator for Mem
/// Mem 反向迭代器
pub struct MemTableRevIter<'a>(btree_map::Range<'a, Key, Pos>);
def_mem_iter!(MemTableRevIter, next_back);

impl MemInner {
  /// Forward range query [start, end)
  /// 正向范围查询 [start, end)
  #[inline]
  pub fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MemTableIter<'_> {
    MemTableIter(self.data.range::<[u8], _>((start, end)))
  }

  /// Reverse range query (end, start]
  /// 反向范围查询 (end, start]
  #[inline]
  pub fn rev_range(&self, end: Bound<&[u8]>, start: Bound<&[u8]>) -> MemTableRevIter<'_> {
    MemTableRevIter(self.data.range::<[u8], _>((start, end)))
  }

  /// Iterate all entries ascending
  /// 升序迭代所有条目
  #[inline]
  pub fn iter(&self) -> MemTableIter<'_> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate all entries descending
  /// 降序迭代所有条目
  #[inline]
  pub fn rev_iter(&self) -> MemTableRevIter<'_> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }
}

impl<F: jdb_base::sst::Flush, N: jdb_base::sst::OnFlush> Mems<F, N> {
  /// Forward range query [start, end)
  /// 正向范围查询 [start, end)
  #[inline]
  pub fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeIter<'_> {
    self.merge_range(start, end)
  }

  /// Reverse range query (end, start]
  /// 反向范围查询 (end, start]
  #[inline]
  pub fn rev_range(&self, end: Bound<&[u8]>, start: Bound<&[u8]>) -> MergeRevIter<'_> {
    self.merge_rev_range(start, end)
  }

  /// Iterate all entries ascending
  /// 升序迭代所有条目
  #[inline]
  pub fn iter(&self) -> MergeIter<'_> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate all entries descending
  /// 降序迭代所有条目
  #[inline]
  pub fn rev_iter(&self) -> MergeRevIter<'_> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }
}
