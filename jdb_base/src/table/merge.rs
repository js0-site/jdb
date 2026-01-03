//! MergeIter - Merge multiple sorted sources
//! 合并迭代器 - 合并多个有序源

use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
};

use hipstr::HipByt;

use super::Kv;
use crate::Pos;

/// Item stored in the heap for merge sort
/// 归并排序堆中存储的元素
struct HeapItem {
  key: HipByt<'static>,
  pos: Pos,
  /// Index of the source iterator (for strict ordering)
  /// 源迭代器的索引（用于严格排序）
  src_idx: usize,
}

impl PartialEq for HeapItem {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.src_idx == other.src_idx
  }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for HeapItem {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    // Min-heap logic: smallest key first.
    // If keys are equal, smallest src_idx first (newest source has priority 0).
    // 最小堆逻辑：最小键优先。
    // 若键相同，最小 src_idx 优先（最新源优先级为 0）。
    match self.key.cmp(&other.key) {
      Ordering::Equal => self.src_idx.cmp(&other.src_idx),
      ord => ord,
    }
  }
}

/// Merge iterator for combining multiple sorted sources
/// 合并多个有序源的迭代器
pub struct MergeIter<I> {
  sources: Vec<I>,
  heap: BinaryHeap<Reverse<HeapItem>>,
  last_key: Option<HipByt<'static>>,
  skip_rm: bool,
}

impl<I> MergeIter<I>
where
  I: Iterator<Item = Kv>,
{
  /// Create from multiple sources (priority order: index 0 = newest)
  /// 从多个源创建（优先级：索引 0 = 最新）
  pub fn new(sources: Vec<I>, skip_rm: bool) -> Self {
    let cap = sources.len();
    let mut iters = Vec::with_capacity(cap);
    let mut heap = BinaryHeap::with_capacity(cap);

    for (idx, mut iter) in sources.into_iter().enumerate() {
      if let Some((key, pos)) = iter.next() {
        heap.push(Reverse(HeapItem {
          key,
          pos,
          src_idx: idx,
        }));
      }
      iters.push(iter);
    }

    Self {
      sources: iters,
      heap,
      last_key: None,
      skip_rm,
    }
  }
}

impl<I> Iterator for MergeIter<I>
where
  I: Iterator<Item = Kv>,
{
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    loop {
      // Pop smallest item (min-heap via Reverse)
      // 弹出最小项（通过 Reverse 实现最小堆）
      let Reverse(HeapItem { key, pos, src_idx }) = self.heap.pop()?;

      // Push next item from the same source
      // 从同源推送下一项
      // SAFETY: src_idx comes from enumerate() in new(), bounded by sources.len().
      // 安全：src_idx 来自 new() 中的 enumerate()，受 sources.len() 限制。
      if let Some((next_key, next_pos)) = unsafe { self.sources.get_unchecked_mut(src_idx).next() }
      {
        self.heap.push(Reverse(HeapItem {
          key: next_key,
          pos: next_pos,
          src_idx,
        }));
      }

      // Dedup: skip if key matches previously yielded key
      // 去重：如果键与前一个输出的键匹配，则跳过
      if let Some(last) = &self.last_key
        && *last == key
      {
        continue;
      }

      // Handle tombstone
      // 处理删除标记
      if self.skip_rm && pos.is_tombstone() {
        // Key is deleted, update last_key (move key ownership) and skip
        // 键已删除，更新 last_key（移动键的所有权）并跳过
        self.last_key = Some(key);
        continue;
      }

      // Update last key (O(1) clone with HipByt) and return
      // 更新最后一个键（HipByt 的 O(1) 克隆）并返回
      self.last_key = Some(key.clone());
      return Some((key, pos));
    }
  }
}
