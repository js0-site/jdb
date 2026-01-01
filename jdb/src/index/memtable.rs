//! Memtable - In-memory write buffer
//! 内存写缓冲区
//!
//! BTreeMap based memtable for recent writes.
//! 基于 BTreeMap 的内存表，用于最近的写入。

use std::{collections::BTreeMap, ops::Bound};

use jdb_base::Pos;

/// Entry in memtable
/// 内存表条目
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entry {
  /// Value with position
  /// 值及其位置
  Value(Pos),
  /// Tombstone marker for deletion
  /// 删除标记
  Tombstone,
}

impl Entry {
  /// Check if entry is tombstone
  /// 检查是否为删除标记
  #[inline(always)]
  pub fn is_tombstone(&self) -> bool {
    matches!(self, Entry::Tombstone)
  }

  /// Get position if value
  /// 获取位置（如果是值）
  #[inline(always)]
  pub fn pos(&self) -> Option<Pos> {
    match self {
      Entry::Value(pos) => Some(*pos),
      Entry::Tombstone => None,
    }
  }
}

/// Memtable - In-memory sorted key-value store using BTreeMap
/// 内存表 - 使用 BTreeMap 的内存有序键值存储
///
/// Note: Changed from blart::TreeMap to BTreeMap because blart doesn't support
/// keys where one is a prefix of another (e.g., [0] and [0, 1]).
/// 注意：从 blart::TreeMap 改为 BTreeMap，因为 blart 不支持
/// 一个键是另一个键前缀的情况（如 [0] 和 [0, 1]）。
pub struct Memtable {
  id: u64,
  data: BTreeMap<Box<[u8]>, Entry>,
  size: u64,
}

impl Memtable {
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

  /// Get entry by key
  /// 按键获取条目
  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<&Entry> {
    self.data.get(key)
  }

  /// Put key-value pair
  /// 插入键值对
  #[inline]
  pub fn put(&mut self, key: Box<[u8]>, pos: Pos) {
    let key_len = key.len() as u64;

    if let Some(old) = self.data.insert(key, Entry::Value(pos)) {
      // Replaced existing entry
      // 替换现有条目
      if old.is_tombstone() {
        // Tombstone has no Pos, add Pos size
        // 删除标记没有 Pos，添加 Pos 大小
        self.size += Pos::SIZE as u64;
      }
      // If old was Value, size unchanged (same key, same Pos size)
      // 如果旧的是 Value，大小不变（相同键，相同 Pos 大小）
    } else {
      // New entry
      // 新条目
      self.size += key_len + Pos::SIZE as u64;
    }
  }

  /// Delete key (insert tombstone)
  /// 删除键（插入删除标记）
  #[inline]
  pub fn del(&mut self, key: Box<[u8]>) {
    let key_len = key.len() as u64;

    if let Some(old) = self.data.insert(key, Entry::Tombstone) {
      // Replaced existing entry
      // 替换现有条目
      if !old.is_tombstone() {
        // Remove Pos size
        // 移除 Pos 大小
        self.size -= Pos::SIZE as u64;
      }
      // If old was Tombstone, no change
      // 如果旧的是 Tombstone，无变化
    } else {
      // New tombstone entry
      // 新删除标记条目
      self.size += key_len;
    }
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

  /// Iterate all entries in order
  /// 按顺序迭代所有条目
  #[inline]
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&[u8], &Entry)> {
    self.data.iter().map(|(k, v)| (k.as_ref(), v))
  }

  /// Range query with bounds
  /// 范围查询
  #[inline]
  pub fn range<'a>(
    &'a self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl DoubleEndedIterator<Item = (&'a [u8], &'a Entry)> {
    // Convert bounds to owned for BTreeMap range
    // 转换边界为 BTreeMap range 所需的格式
    let start_owned: Bound<Box<[u8]>> = match start {
      Bound::Included(k) => Bound::Included(k.into()),
      Bound::Excluded(k) => Bound::Excluded(k.into()),
      Bound::Unbounded => Bound::Unbounded,
    };
    let end_owned: Bound<Box<[u8]>> = match end {
      Bound::Included(k) => Bound::Included(k.into()),
      Bound::Excluded(k) => Bound::Excluded(k.into()),
      Bound::Unbounded => Bound::Unbounded,
    };

    self
      .data
      .range((start_owned, end_owned))
      .map(|(k, v)| (k.as_ref(), v))
  }
}
