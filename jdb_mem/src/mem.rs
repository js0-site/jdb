use core::ops::Bound;
use jdb_base::Pos;
use std::borrow::Borrow;

use crate::Map;
use crate::iter::{MapIter, MapRevIter, MergeIter};

/// Memory-resident part of the database with layered maps
/// 数据库的内存储存部分，具有分层映射
#[derive(Default, Debug)]
pub struct Mem {
  /// Current active map for writes
  /// 当前用于写入的活跃 Map
  pub now: Map,
  /// Immutable/older maps pending flush
  /// 等待刷盘的不可变/旧 Map
  pub old: Vec<Map>,
}

impl Mem {
  /// Create a new empty Mem
  /// 创建一个新的空 Mem
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  /// Rotate current map to old maps and initialize a new one
  /// 将当前 Map 轮转到旧 Map 列表并初始化一个新的 Map
  #[cold]
  pub fn rotate(&mut self) {
    let now = std::mem::take(&mut self.now);
    self.old.insert(0, now);
  }
}

impl jdb_base::Mem for Mem {
  type Key<'a> = &'a [u8];
  type Iter<'a> = MergeIter<'a, MapIter<'a>>;
  type RevIter<'a> = MergeIter<'a, MapRevIter<'a>>;

  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.now.put(key, pos);
  }

  #[inline]
  fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos> {
    let key = key.borrow();
    self.now.get(key).or_else(|| {
      self
        .old
        .iter()
        .find_map(|map| map.get(key))
    })
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.iter());
    for map in &self.old {
      iters.push(map.iter());
    }
    MergeIter::new(iters, false)
  }

  #[inline]
  fn rev_iter(&self) -> Self::RevIter<'_> {
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.rev_iter());
    for map in &self.old {
      iters.push(map.rev_iter());
    }
    MergeIter::new(iters, true)
  }

  #[inline]
  fn range<Start: Borrow<[u8]>, End: Borrow<[u8]>>(
    &self,
    start: Bound<Start>,
    end: Bound<End>,
  ) -> Self::Iter<'_> {
    let (s, e) = map_bounds(&start, &end);
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.range(s, e));
    for map in &self.old {
      iters.push(map.range(s, e));
    }
    MergeIter::new(iters, false)
  }

  #[inline]
  fn rev_range<Start: Borrow<[u8]>, End: Borrow<[u8]>>(
    &self,
    end: Bound<End>,
    start: Bound<Start>,
  ) -> Self::RevIter<'_> {
    let (s, e) = map_bounds(&start, &end);
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.rev_range(e, s));
    for map in &self.old {
      iters.push(map.rev_range(e, s));
    }
    MergeIter::new(iters, true)
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
