//! Memtable - In-memory write buffer
//! 内存写缓冲区
//!
//! Adaptive radix tree based memtable for recent writes.
//! 基于自适应基数树的内存表，用于最近的写入。

use std::ops::Bound;

use blart::TreeMap;
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

/// Memtable - In-memory sorted key-value store using adaptive radix tree
/// 内存表 - 使用自适应基数树的内存有序键值存储
pub struct Memtable {
  id: u64,
  data: TreeMap<Box<[u8]>, Entry>,
  size: u64,
}

impl Memtable {
  /// Create new memtable with ID
  /// 创建新的内存表
  #[inline]
  pub fn new(id: u64) -> Self {
    Self {
      id,
      data: TreeMap::new(),
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
    let entry_size = key_len + Pos::SIZE as u64;

    // blart's try_insert returns Ok(None) for new key, Ok(Some(old)) for update
    // blart 的 try_insert 对新键返回 Ok(None)，对更新返回 Ok(Some(old))
    match self.data.try_insert(key, Entry::Value(pos)) {
      Ok(None) => {
        // New entry
        // 新条目
        self.size += entry_size;
      }
      Ok(Some(old)) => {
        // Replaced existing entry
        // 替换现有条目
        match old {
          Entry::Value(_) => {
            // Size unchanged (same key, same Pos size)
            // 大小不变（相同键，相同 Pos 大小）
          }
          Entry::Tombstone => {
            // Tombstone has no Pos, add Pos size
            // 删除标记没有 Pos，添加 Pos 大小
            self.size += Pos::SIZE as u64;
          }
        }
      }
      Err(_) => {
        // Key is prefix of existing key or vice versa - should not happen with proper keys
        // 键是现有键的前缀或反之 - 使用正确的键不应发生
      }
    }
  }

  /// Delete key (insert tombstone)
  /// 删除键（插入删除标记）
  #[inline]
  pub fn del(&mut self, key: Box<[u8]>) {
    let key_len = key.len() as u64;

    match self.data.try_insert(key, Entry::Tombstone) {
      Ok(None) => {
        // New tombstone entry
        // 新删除标记条目
        self.size += key_len;
      }
      Ok(Some(old)) => {
        match old {
          Entry::Value(_) => {
            // Remove Pos size
            // 移除 Pos 大小
            self.size -= Pos::SIZE as u64;
          }
          Entry::Tombstone => {
            // Already tombstone, no change
            // 已经是删除标记，无变化
          }
        }
      }
      Err(_) => {
        // Key is prefix of existing key or vice versa
        // 键是现有键的前缀或反之
      }
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
  ///
  /// Note: Uses iter() with filtering to avoid blart range() bugs.
  /// 注意：使用 iter() 加过滤来避免 blart range() 的 bug。
  #[inline]
  pub fn range<'a>(
    &'a self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl DoubleEndedIterator<Item = (&'a [u8], &'a Entry)> {
    // Convert bounds to owned for comparison
    // 转换边界为拥有的数据用于比较
    let start_owned: Option<Box<[u8]>> = match start {
      Bound::Included(k) | Bound::Excluded(k) => Some(k.into()),
      Bound::Unbounded => None,
    };
    let end_owned: Option<Box<[u8]>> = match end {
      Bound::Included(k) | Bound::Excluded(k) => Some(k.into()),
      Bound::Unbounded => None,
    };
    let start_inclusive = matches!(start, Bound::Included(_));
    let end_inclusive = matches!(end, Bound::Included(_));

    self.data.iter().filter_map(move |(k, v)| {
      let key = k.as_ref();

      // Check start bound
      // 检查起始边界
      if let Some(ref s) = start_owned {
        if start_inclusive {
          if key < s.as_ref() {
            return None;
          }
        } else if key <= s.as_ref() {
          return None;
        }
      }

      // Check end bound
      // 检查结束边界
      if let Some(ref e) = end_owned {
        if end_inclusive {
          if key > e.as_ref() {
            return None;
          }
        } else if key >= e.as_ref() {
          return None;
        }
      }

      Some((key, v))
    })
  }
}
