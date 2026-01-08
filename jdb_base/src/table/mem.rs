//! Kv - Key-Value abstractions and Table trait
//! Kv - 键值对抽象与表接口 Trait

use std::ops::Bound;
use crate::table::Kv;
use crate::Pos;

/// Iteration order
/// 迭代顺序
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
  /// Ascending order
  /// 升序
  Asc,
  /// Descending order
  /// 降序
  Desc,
}

/// Synchronous query trait for in-memory tables (Mem)
/// 同步查询 trait，用于内存表
pub trait Table {
  /// Iterator type for range queries (GATs for zero-copy)
  /// 范围查询的迭代器类型（GATs 实现零拷贝）
  type Iter<'a>: Iterator<Item = Kv>
  where
    Self: 'a;

  /// Get entry by key
  /// 按键获取条目
  fn get(&self, key: &[u8]) -> Option<Pos>;

  /// Range query with bounds
  /// 范围查询
  ///
  /// - `Asc`: iterates from `start` to `end`
  /// - `Desc`: iterates from `end` to `start`
  ///
  /// - `Asc`: 从 `start` 遍历到 `end`
  /// - `Desc`: 从 `end` 遍历到 `start`
  fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>, order: Order) -> Self::Iter<'_>;

  /// Iterate entries with prefix
  /// 迭代带前缀的条目
  #[inline]
  fn prefix(&self, prefix: &[u8], order: Order) -> Self::Iter<'_> {
    let start = Bound::Included(prefix);
    // The iterator implementation must copy the bound if it needs to persist it,
    // as the result of prefix_end is temporary.
    // 迭代器实现必须复制边界（如果需要持久化），因为 prefix_end 的结果是临时的。
    match prefix_end(prefix) {
      Some(end) => self.range(start, Bound::Excluded(end.as_ref()), order),
      None => self.range(start, Bound::Unbounded, order),
    }
  }

  /// Iterate over all entries in ascending order
  /// 按升序迭代所有条目
  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    self.range(Bound::Unbounded, Bound::Unbounded, Order::Asc)
  }

  /// Iterate over all entries in descending order
  /// 按降序迭代所有条目
  #[inline]
  fn rev_iter(&self) -> Self::Iter<'_> {
    self.range(Bound::Unbounded, Bound::Unbounded, Order::Desc)
  }
}

/// Mutable table trait for write operations
/// 可变表 trait，用于写操作
pub trait TableMut: Table {
  /// Put key-value pair
  /// 插入键值对
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos);

  /// Remove key (insert tombstone)
  /// 删除键（插入删除标记）
  fn rm(&mut self, key: impl Into<Box<[u8]>>);
}

/// Calculate exclusive end bound for prefix
/// 计算前缀的排他结束边界
#[inline]
pub fn prefix_end(prefix: &[u8]) -> Option<Box<[u8]>> {
  // Find last non-0xff byte from end
  // 从末尾找到最后一个非 0xff 字节
  let pos = prefix.iter().rposition(|&b| b < 0xff)?;

  // Construct new key: prefix[..pos] + (prefix[pos] + 1)
  // 构造新 Key
  let mut end = Vec::with_capacity(pos + 1);
  end.extend_from_slice(&prefix[..pos]);
  end.push(prefix[pos] + 1);
  Some(end.into_boxed_slice())
}
