//! PeekIter - Peekable iterator trait for merge heap
//! 可窥视迭代器 trait，用于合并堆

use super::Kv;

/// Peekable iterator for merge heap
/// 用于合并堆的可窥视迭代器
pub trait PeekIter: Ord {
  /// Peek current item
  /// 查看当前元素
  fn peek(&self) -> Option<&Kv>;

  /// Take current item and advance
  /// 取出当前元素并前进
  fn take(&mut self) -> Option<Kv>;
}
