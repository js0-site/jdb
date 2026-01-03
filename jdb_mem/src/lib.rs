//! jdb_mem - In-memory table implementation
//! 内存表实现
//!
//! BTreeMap based memtable for recent writes.
//! 基于 BTreeMap 的内存表，用于最近的写入。

use std::{collections::BTreeMap, ops::Bound};

use hipstr::HipByt;
use jdb_base::{
  Pos,
  table::{Kv, Table, TableMut},
};

/// Memtable - In-memory sorted key-value store
/// 内存表 - 内存有序键值存储
///
/// Note: Uses BTreeMap because blart doesn't support
/// keys where one is a prefix of another (e.g., [0] and [0, 1]).
/// 注意：使用 BTreeMap，因为 blart 不支持
/// 一个键是另一个键前缀的情况（如 [0] 和 [0, 1]）。
pub struct Mem {
  id: u64,
  data: BTreeMap<HipByt<'static>, Pos>,
  size: u64,
}

impl Mem {
  /// Create new memtable with ID
  /// 创建新的内存表
  #[inline]
  pub fn new(id: u64) -> Self {
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

/// Iterator for Mem range queries
/// Mem 范围查询迭代器
pub struct MemIter {
  data: Vec<Kv>,
  idx: usize,
}

impl Iterator for MemIter {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.idx < self.data.len() {
      let item = self.data[self.idx].clone();
      self.idx += 1;
      Some(item)
    } else {
      None
    }
  }
}

impl DoubleEndedIterator for MemIter {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.idx < self.data.len() {
      let item = self.data[self.data.len() - 1].clone();
      self.data.pop();
      Some(item)
    } else {
      None
    }
  }
}

impl Table for Mem {
  type Iter = MemIter;

  #[inline]
  fn get(&self, key: &[u8]) -> Option<Pos> {
    self.data.get(key).copied()
  }

  #[inline]
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter {
    // Collect to Vec to avoid lifetime issues
    // 收集到 Vec 以避免生命周期问题
    let data: Vec<Kv> = self
      .data
      .range::<[u8], _>((start, end))
      .map(|(k, &v)| (k.clone(), v))
      .collect();
    MemIter { data, idx: 0 }
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
