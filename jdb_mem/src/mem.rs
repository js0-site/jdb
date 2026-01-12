use core::ops::RangeBounds;
use std::borrow::Borrow;

use jdb_base::{Pos, query::start_end};

use crate::{
  Map,
  iter::{Asc, Desc, MapIter, MapRevIter, MergeIter},
};

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
  /// Key type for the memory table
  /// 内存表的键类型
  type Key<'a> = &'a [u8];

  /// Forward iterator type
  /// 正向迭代器类型
  type Iter<'a> = MergeIter<'a, MapIter<'a>, Asc>;

  /// Reverse iterator type
  /// 反向迭代器类型
  type RevIter<'a> = MergeIter<'a, MapRevIter<'a>, Desc>;

  /// Insert a key-position pair into the active map
  /// 将键值位置对插入活跃 Map
  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.now.put(key, pos);
  }

  /// Get position by key across all layers (newest first)
  /// 在所有层级中按键获取位置（由新到旧）
  #[inline]
  fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos> {
    let key = key.borrow();
    self
      .now
      .get(key)
      .or_else(|| self.old.iter().find_map(|map| map.get(key)))
  }

  /// Get forward iterator over all layers
  /// 获取涵盖所有层级的正向迭代器
  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.iter());
    for map in &self.old {
      iters.push(map.iter());
    }
    MergeIter::new(iters)
  }

  /// Get reverse iterator over all layers
  /// 获取涵盖所有层级的反向迭代器
  #[inline]
  fn rev_iter(&self) -> Self::RevIter<'_> {
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.rev_iter());
    for map in &self.old {
      iters.push(map.rev_iter());
    }
    MergeIter::new(iters)
  }

  /// Get range iterator over all layers
  /// 获取涵盖所有层级的范围迭代器
  #[inline]
  fn range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::Iter<'_> {
    let bounds = start_end(&range);
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.range::<[u8]>(bounds));
    for map in &self.old {
      iters.push(map.range::<[u8]>(bounds));
    }
    MergeIter::new(iters)
  }

  /// Get reverse range iterator over all layers
  /// 获取涵盖所有层级的反向范围迭代器
  #[inline]
  fn rev_range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::RevIter<'_> {
    let bounds = start_end(&range);
    let mut iters = Vec::with_capacity(self.old.len() + 1);
    iters.push(self.now.rev_range::<[u8]>(bounds));
    for map in &self.old {
      iters.push(map.rev_range::<[u8]>(bounds));
    }
    MergeIter::new(iters)
  }
}
