//! In-memory table implementation
//! 内存表实现
//!
//! BTreeMap based memtable for recent writes.
//! 基于 BTreeMap 的内存表，用于最近的写入。

use std::{
  cmp::Ordering,
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

impl Eq for Mem {}

impl PartialEq for Mem {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl PartialOrd for Mem {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Mem {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    // Higher id = newer = higher priority
    // id 越大 = 越新 = 优先级越高
    self.id.cmp(&other.id)
  }
}

/// Iterator for Mem range queries with table ID for merge priority
/// Mem 范围查询迭代器，带表 ID 用于合并优先级
pub struct MemIter<'a> {
  id: u64,
  inner: btree_map::Range<'a, Box<[u8]>, Pos>,
  /// Current buffered item for peek/comparison
  /// 当前缓冲元素，用于查看/比较
  cur: Option<Kv>,
}

impl<'a> MemIter<'a> {
  /// Get table ID
  /// 获取表 ID
  #[inline]
  pub fn id(&self) -> u64 {
    self.id
  }

  /// Peek current item without consuming
  /// 查看当前元素但不消费
  #[inline]
  pub fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  /// Advance to next item
  /// 前进到下一个元素
  #[inline]
  pub fn advance(&mut self) {
    self.cur = self.inner.next().map(|(k, &v)| (k.clone(), v));
  }

  /// Take current item and advance
  /// 取出当前元素并前进
  #[inline]
  pub fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.advance();
    item
  }
}

impl Iterator for MemIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    // If cur is buffered (for MergeIter), use it first
    // 如果 cur 已缓冲（用于 MergeIter），先使用它
    if let Some(item) = self.cur.take() {
      self.advance();
      return Some(item);
    }
    self.inner.next().map(|(k, &v)| (k.clone(), v))
  }
}

impl DoubleEndedIterator for MemIter<'_> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    // For reverse iteration, drain cur first if it exists
    // 反向迭代时，如果 cur 存在先清空它
    if self.cur.is_some() {
      // cur holds the first element, we need to return it last in reverse
      // cur 持有第一个元素，反向时需要最后返回
      if let Some(item) = self.inner.next_back() {
        return Some((item.0.clone(), *item.1));
      }
      // No more items in inner, return cur
      // inner 没有更多元素，返回 cur
      return self.cur.take();
    }
    self.inner.next_back().map(|(k, &v)| (k.clone(), v))
  }
}

impl FusedIterator for MemIter<'_> {}

impl Eq for MemIter<'_> {}

impl PartialEq for MemIter<'_> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl PartialOrd for MemIter<'_> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemIter<'_> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    match (&self.cur, &other.cur) {
      (Some((k1, _)), Some((k2, _))) => {
        // Reverse key order for min-heap (BinaryHeap is max-heap)
        // 反转键顺序以实现最小堆（BinaryHeap 是最大堆）
        match k2.as_ref().cmp(k1.as_ref()) {
          // Same key: higher id (newer) wins
          // 相同键：id 大的（更新）胜出
          Ordering::Equal => self.id.cmp(&other.id),
          ord => ord,
        }
      }
      // None is "infinite", so it's always greater (lower priority)
      // None 是"无穷大"，所以总是更大（优先级更低）
      (Some(_), None) => Ordering::Greater,
      (None, Some(_)) => Ordering::Less,
      (None, None) => Ordering::Equal,
    }
  }
}

impl jdb_base::table::PeekIter for MemIter<'_> {
  #[inline]
  fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  #[inline]
  fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.advance();
    item
  }
}

/// Reverse iterator for Mem range queries
/// Mem 范围查询的反向迭代器
pub struct MemRevIter<'a> {
  id: u64,
  inner: btree_map::Range<'a, Box<[u8]>, Pos>,
  /// Current buffered item for peek/comparison
  /// 当前缓冲元素，用于查看/比较
  cur: Option<Kv>,
}

impl<'a> MemRevIter<'a> {
  /// Get table ID
  /// 获取表 ID
  #[inline]
  pub fn id(&self) -> u64 {
    self.id
  }

  /// Peek current item without consuming
  /// 查看当前元素但不消费
  #[inline]
  pub fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  /// Advance to next item (from back)
  /// 前进到下一个元素（从后向前）
  #[inline]
  pub fn advance(&mut self) {
    self.cur = self.inner.next_back().map(|(k, &v)| (k.clone(), v));
  }

  /// Take current item and advance
  /// 取出当前元素并前进
  #[inline]
  pub fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.advance();
    item
  }
}

impl Iterator for MemRevIter<'_> {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.cur.is_some() {
      let item = self.cur.take();
      self.advance();
      item
    } else {
      self.inner.next_back().map(|(k, &v)| (k.clone(), v))
    }
  }
}

impl FusedIterator for MemRevIter<'_> {}

impl Eq for MemRevIter<'_> {}

impl PartialEq for MemRevIter<'_> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl PartialOrd for MemRevIter<'_> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemRevIter<'_> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    match (&self.cur, &other.cur) {
      (Some((k1, _)), Some((k2, _))) => {
        // For descending: larger key first, so reverse comparison
        // 降序：大键优先，所以反转比较
        match k1.as_ref().cmp(k2.as_ref()) {
          // Same key: higher id (newer) wins
          // 相同键：id 大的（更新）胜出
          Ordering::Equal => self.id.cmp(&other.id),
          ord => ord,
        }
      }
      // None is "infinite", so it's always greater (lower priority)
      // None 是"无穷大"，所以总是更大（优先级更低）
      (Some(_), None) => Ordering::Greater,
      (None, Some(_)) => Ordering::Less,
      (None, None) => Ordering::Equal,
    }
  }
}

impl jdb_base::table::PeekIter for MemRevIter<'_> {
  #[inline]
  fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  #[inline]
  fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.advance();
    item
  }
}

impl Table for Mem {
  type Iter<'a> = MemIter<'a>;

  #[inline]
  fn get(&self, key: &[u8]) -> Option<Pos> {
    self.data.get(key).copied()
  }

  #[inline]
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter<'_> {
    let mut iter = MemIter {
      id: self.id,
      inner: self.data.range::<[u8], _>((start, end)),
      cur: None,
    };
    iter.advance();
    iter
  }
}

impl Mem {
  /// Create reverse range iterator
  /// 创建反向范围迭代器
  #[inline]
  pub fn rev_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MemRevIter<'_> {
    let mut iter = MemRevIter {
      id: self.id,
      inner: self.data.range::<[u8], _>((start, end)),
      cur: None,
    };
    iter.advance();
    iter
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
