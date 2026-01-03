//! Table - Abstract query interface for index tables
//! 表 - 索引表的抽象查询接口

use std::ops::Bound;

use hipstr::HipByt;

use crate::Pos;

/// Kv pair with smart byte string key (O(1) clone, inline small keys)
/// 带智能字节串 Key 的键值对（O(1) 克隆，小 Key 内联）
pub type Kv = (HipByt<'static>, Pos);

/// Query trait for index table (read-only)
/// 索引表查询 trait（只读）
pub trait Table {
  /// Iterator type for range/iter queries (must support reverse iteration)
  /// 范围/迭代查询的迭代器类型（必须支持反向迭代）
  type Iter: Iterator<Item = Kv> + DoubleEndedIterator;

  /// Get entry by key
  /// 按键获取条目
  fn get(&self, key: &[u8]) -> Option<Pos>;

  /// Range query with bounds
  /// 范围查询
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::Iter;

  /// Iterate all entries
  /// 迭代所有条目
  fn iter(&self) -> Self::Iter {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate entries with prefix
  /// 迭代带前缀的条目
  fn prefix(&self, prefix: &[u8]) -> Self::Iter {
    let start = Bound::Included(prefix);
    match prefix_end(prefix) {
      Some(end) => self.range(start, Bound::Excluded(end.as_ref())),
      None => self.range(start, Bound::Unbounded),
    }
  }
}

/// Mutable table trait for write operations
/// 可变表 trait，用于写操作
pub trait TableMut: Table {
  /// Put key-value pair
  /// 插入键值对
  fn put(&mut self, key: impl Into<HipByt<'static>>, pos: Pos);

  /// Remove key (insert tombstone)
  /// 删除键（插入删除标记）
  fn rm(&mut self, key: impl Into<HipByt<'static>>, wal_id: u64, offset: u64);
}

/// Calculate exclusive end bound for prefix
/// 计算前缀的排他结束边界
#[inline]
fn prefix_end(prefix: &[u8]) -> Option<HipByt<'static>> {
  for (i, &b) in prefix.iter().enumerate().rev() {
    if b < 0xff {
      let mut end = Vec::with_capacity(i + 1);
      end.extend_from_slice(&prefix[..i]);
      end.push(b + 1);
      return Some(HipByt::from(end));
    }
  }
  None
}
