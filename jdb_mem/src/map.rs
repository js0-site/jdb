use core::ops::Bound;
use jdb_base::{Mem, Pos};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::iter::{MapIter, MapRevIter};

/// In-memory table implementation using BTreeMap
/// 使用 BTreeMap 实现的内存表
#[derive(Default, Debug)]
pub struct Map {
  /// Internal storage for key-position pairs
  /// 键值位置对的内部存储
  pub inner: BTreeMap<Box<[u8]>, Pos>,
  /// List of discarded entries for future SST GC
  /// 丢弃条目列表，用于未来的 SST 垃圾回收
  pub discard_li: Vec<(Box<[u8]>, Pos)>,
}

impl Map {
  /// Create a new empty Map
  /// 创建一个新的空 Map
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }
}

impl Mem for Map {
  type Key<'a> = &'a [u8];
  type Iter<'a> = MapIter<'a>;
  type RevIter<'a> = MapRevIter<'a>;

  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    let key = key.into();
    match self.inner.entry(key) {
      Entry::Occupied(mut e) => {
        let old_pos = e.insert(pos);
        self.discard_li.push((e.key().clone(), old_pos));
      }
      Entry::Vacant(e) => {
        e.insert(pos);
      }
    }
  }

  #[inline]
  fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos> {
    self.inner.get(key.borrow()).copied()
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    MapIter(self.inner.range::<[u8], _>(..))
  }

  #[inline]
  fn rev_iter(&self) -> Self::RevIter<'_> {
    MapRevIter(self.inner.range::<[u8], _>(..).rev())
  }

  #[inline]
  fn range<Start: Borrow<[u8]>, End: Borrow<[u8]>>(
    &self,
    start: Bound<Start>,
    end: Bound<End>,
  ) -> Self::Iter<'_> {
    let (s, e) = map_bounds(&start, &end);
    MapIter(self.inner.range::<[u8], _>((s, e)))
  }

  #[inline]
  fn rev_range<Start: Borrow<[u8]>, End: Borrow<[u8]>>(
    &self,
    end: Bound<End>,
    start: Bound<Start>,
  ) -> Self::RevIter<'_> {
    let (s, e) = map_bounds(&start, &end);
    MapRevIter(self.inner.range::<[u8], _>((s, e)).rev())
  }
}

/// Helper to map generic bounds to slice bounds
/// 将泛型边界映射到切片边界的辅助函数
#[inline]
fn map_bounds<'a, S: Borrow<[u8]>, E: Borrow<[u8]>>(
  start: &'a Bound<S>,
  end: &'a Bound<E>,
) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
  let s = match start {
    Bound::Included(b) => Bound::Included(b.borrow()),
    Bound::Excluded(b) => Bound::Excluded(b.borrow()),
    Bound::Unbounded => Bound::Unbounded,
  };
  let e = match end {
    Bound::Included(b) => Bound::Included(b.borrow()),
    Bound::Excluded(b) => Bound::Excluded(b.borrow()),
    Bound::Unbounded => Bound::Unbounded,
  };
  (s, e)
}
