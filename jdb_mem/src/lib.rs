//! In-memory table implementation
//! 内存表实现
//!
//! BTreeMap based memtable for recent writes.
//! 基于 BTreeMap 的内存表，用于最近的写入。

use std::{
  collections::{BTreeMap, btree_map},
  iter::FusedIterator,
  ops::Bound,
};

use jdb_base::{
  Pos, id,
  table::{Kv, Table, TableMut},
};

/// Memtable - In-memory sorted key-value store
/// 内存表 - 内存有序键值存储
///
/// Note: Uses BTreeMap because radix trees don't support keys where one is a prefix of another.
/// 注意：使用 BTreeMap，因为基数树不支持
/// 一个键是另一个键前缀的情况（如 [0] 和 [0, 1]）。
pub struct Mem {
  id: u64,
  data: BTreeMap<Box<[u8]>, Pos>,
  size: u64,
}

impl Mem {
  /// Create new memtable with auto-generated ID
  /// 创建新的内存表（自动生成 ID）
  #[inline]
  pub fn new() -> Self {
    Self {
      id: id(),
      data: BTreeMap::new(),
      size: 0,
    }
  }

  /// Get memtable ID
  /// 获取内存表 ID
  #[inline]
  pub fn id(&self) -> u64 {
    self.id
  }

  /// Get approximate size in bytes
  /// 获取近似大小（字节）
  #[inline]
  pub fn size(&self) -> u64 {
    self.size
  }

  /// Get entry count
  /// 获取条目数量
  #[inline]
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Check if empty
  /// 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  /// Internal upsert helper to update size tracking
  /// 内部插入更新辅助函数，用于追踪大小
  #[inline]
  fn upsert(&mut self, key: Box<[u8]>, val: Pos) {
    let key_len = key.len() as u64;
    // Only increase size for new keys to keep approximation simple and fast
    // 仅在新增键时增加大小，以保持近似计算简单快速
    if self.data.insert(key, val).is_none() {
      self.size += key_len + Pos::SIZE as u64;
    }
  }
}

/// Iterator for Mem range queries (zero-copy, lazy evaluation)
/// Mem 范围查询迭代器（零拷贝，惰性求值）
pub struct MemIter<'a> {
  inner: btree_map::Range<'a, Box<[u8]>, Pos>,
}

impl Iterator for MemIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|(k, &v)| (k.clone(), v))
  }
}

impl DoubleEndedIterator for MemIter<'_> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.inner.next_back().map(|(k, &v)| (k.clone(), v))
  }
}

impl FusedIterator for MemIter<'_> {}

impl Table for Mem {
  type Iter<'a> = MemIter<'a>;

  #[inline]
  fn get(&self, key: &[u8]) -> Option<Pos> {
    self.data.get(key).copied()
  }

  #[inline]
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter<'_> {
    MemIter {
      inner: self.data.range::<[u8], _>((start, end)),
    }
  }
}

impl Default for Mem {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl TableMut for Mem {
  #[inline]
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.upsert(key.into(), pos);
  }

  #[inline]
  fn rm(&mut self, key: impl Into<Box<[u8]>>) {
    self.upsert(key.into(), Pos::tombstone(id(), 0, 0));
  }
}
