use core::ops::RangeBounds;
use std::{borrow::Borrow, collections::BTreeMap};

use jdb_base::{Pos, query::start_end};

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
  pub const ENTRY_OVERHEAD: usize = 32;

  /// Create a new empty Map
  /// 创建一个新的空 Map
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  /// Get position by key
  /// 通过键获取位置
  #[inline]
  pub fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos> {
    self.inner.get(key.borrow()).copied()
  }

  /// Get forward iterator
  /// 获取正向迭代器
  #[inline]
  pub fn iter(&self) -> MapIter<'_> {
    MapIter(self.inner.range::<[u8], _>(..))
  }

  /// Get reverse iterator
  /// 获取反向迭代器
  #[inline]
  pub fn rev_iter(&self) -> MapRevIter<'_> {
    MapRevIter(self.inner.range::<[u8], _>(..).rev())
  }

  /// Get range iterator
  /// 获取范围迭代器
  #[inline]
  pub fn range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> MapIter<'_> {
    MapIter(self.inner.range::<[u8], _>(start_end(&range)))
  }

  /// Get reverse range iterator
  /// 获取反向范围迭代器
  #[inline]
  pub fn rev_range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> MapRevIter<'_> {
    MapRevIter(self.inner.range::<[u8], _>(start_end(&range)).rev())
  }
}
