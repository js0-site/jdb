//! Kv - Key-Value abstractions and Table trait
//! Kv - 键值对抽象与表接口 Trait

use std::ops::Bound;

use crate::{Kv, Pos, prefix_end};

/// Synchronous query trait for in-memory tables (Mem)
/// 同步查询 trait，用于内存表
pub trait Mem {
  /// Forward iterator type
  /// 正向迭代器类型
  type Iter<'a>: Iterator<Item = Kv>
  where
    Self: 'a;

  /// Reverse iterator type
  /// 反向迭代器类型
  type RevIter<'a>: Iterator<Item = Kv>
  where
    Self: 'a;

  /// Get entry by key
  /// 按键获取条目
  fn get(&self, key: &[u8]) -> Option<Pos>;

  /// Forward range query [start, end)
  /// 正向范围查询 [start, end)
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter<'_>;

  /// Reverse range query (end, start]
  /// 反向范围查询 (end, start]
  fn rev_range(&self, end: Bound<&[u8]>, start: Bound<&[u8]>) -> Self::RevIter<'_>;

  /// Forward prefix scan
  /// 正向前缀扫描
  #[inline]
  fn prefix(&self, prefix: &[u8]) -> Self::Iter<'_> {
    let start = Bound::Included(prefix);
    match prefix_end(prefix) {
      Some(end) => self.range(start, Bound::Excluded(end.as_ref())),
      None => self.range(start, Bound::Unbounded),
    }
  }

  /// Reverse prefix scan
  /// 反向前缀扫描
  #[inline]
  fn rev_prefix(&self, prefix: &[u8]) -> Self::RevIter<'_> {
    let start = Bound::Included(prefix);
    match prefix_end(prefix) {
      Some(end) => self.rev_range(Bound::Excluded(end.as_ref()), start),
      None => self.rev_range(Bound::Unbounded, start),
    }
  }

  /// Iterate all entries ascending
  /// 升序迭代所有条目
  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate all entries descending
  /// 降序迭代所有条目
  #[inline]
  fn rev_iter(&self) -> Self::RevIter<'_> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Put key-value pair
  /// 插入键值对
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos);

  /// Remove key (insert tombstone)
  /// 删除键（插入删除标记）
  fn rm(&mut self, key: impl Into<Box<[u8]>>);
}
