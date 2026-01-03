//! jdb_mem - In-memory table implementation
//! 内存表实现
//!
//! BTreeMap based memtable for recent writes.
//! 基于 BTreeMap 的内存表，用于最近的写入。

use std::{
  collections::{BTreeMap, btree_map},
  iter::FusedIterator,
  ops::Bound,
};

use hipstr::HipByt;
use jdb_base::{
  Pos,
  table::{Kv, Table, TableMut},
};

/// Memtable - In-memory sorted key-value store
/// 内存表 - 内存有序键值存储
///
/// Note: Uses BTreeMap because radix trees don't support
/// keys where one is a prefix of another (e.g., [0] and [0, 1]).
/// 注意：使用 BTreeMap，因为基数树不支持
/// 一个键是另一个键前缀的情况（如 [0] 和 [0, 1]）。
pub struct Mem {
  id: u64,
  data: BTreeMap<HipByt<'static>, Pos>,
  size: u64,
}

impl Mem {
  /// Create new memtable with ID
  /// 创建新的内存表
  #[inline(always)]
  pub const fn new(id: u64) -> Self {
    Self {
      id,
      data: BTreeMap::new(),
      size: 0,
    }
  }

  /// Get memtable ID
  /// 获取内存表 ID
  #[inline(always)]
  pub fn id(&self) -> u64 {
    self.id
  }

  /// Get approximate size in bytes
  /// 获取近似大小（字节）
  #[inline(always)]
  pub fn size(&self) -> u64 {
    self.size
  }

  /// Get entry count
  /// 获取条目数量
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Check if empty
  /// 检查是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }
}

/// Iterator for Mem range queries (zero-copy, lazy evaluation)
/// Mem 范围查询迭代器（零拷贝，惰性求值）
pub struct MemIter<'a> {
  iter: btree_map::Range<'a, HipByt<'static>, Pos>,
}

impl<'a> Iterator for MemIter<'a> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    // HipByt clone is cheap (ref-counted), Pos is Copy
    // HipByt 克隆很廉价（引用计数），Pos 是 Copy 类型
    self.iter.next().map(|(k, &v)| (k.clone(), v))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl DoubleEndedIterator for MemIter<'_> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|(k, &v)| (k.clone(), v))
  }
}

impl ExactSizeIterator for MemIter<'_> {}

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
      iter: self.data.range::<[u8], _>((start, end)),
    }
  }
}

impl TableMut for Mem {
  #[inline]
  fn put(&mut self, key: impl Into<HipByt<'static>>, pos: Pos) {
    let key = key.into();
    let key_len = key.len() as u64;
    if self.data.insert(key, pos).is_none() {
      // New entry
      // 新条目
      self.size += key_len + Pos::SIZE as u64;
    }
    // If replaced, size unchanged (same key, same Pos size)
    // 如果替换，大小不变（相同键，相同 Pos 大小）
  }

  #[inline]
  fn rm(&mut self, key: impl Into<HipByt<'static>>, wal_id: u64, offset: u64) {
    let key = key.into();
    let key_len = key.len() as u64;
    if self
      .data
      .insert(key, Pos::tombstone(wal_id, offset))
      .is_none()
    {
      // New tombstone entry
      // 新删除标记条目
      self.size += key_len + Pos::SIZE as u64;
    }
  }
}
