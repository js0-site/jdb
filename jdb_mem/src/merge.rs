//! Merge iterator for multiple sorted iterators
//! 多路有序迭代器归并

use std::{
  cmp::Ordering,
  collections::{BinaryHeap, binary_heap::PeekMut, btree_map},
  iter::FusedIterator,
  marker::PhantomData,
  mem,
  ops::Bound,
};

use jdb_base::Pos;

use crate::{Kv, Mem};

/// A source combines a Mem and its iterator
/// 源结构体结合了 Mem 和迭代器
struct Source {
  iter: btree_map::Range<'static, Box<[u8]>, Pos>,
  _mem: Mem,
}

/// Heap entry for merge iteration
/// 归并迭代的堆条目
#[derive(Debug, Clone)]
struct Entry {
  key: Box<[u8]>,
  pos: Pos,
  id: u64,
  idx: usize,
}

impl Eq for Entry {}

impl PartialEq for Entry {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.id == other.id
  }
}

/// Wrapper for min-heap ordering (forward iteration)
/// 最小堆排序包装（正向迭代）
struct MinEntry(Entry);

impl Eq for MinEntry {}

impl PartialEq for MinEntry {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl PartialOrd for MinEntry {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MinEntry {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    // Min-heap: reverse key order, same key -> larger id first
    // 最小堆：反转 key 顺序，相同 key 时大 id 优先
    match other.0.key.cmp(&self.0.key) {
      Ordering::Equal => self.0.id.cmp(&other.0.id),
      ord => ord,
    }
  }
}

/// Wrapper for max-heap ordering (backward iteration)
/// 最大堆排序包装（反向迭代）
struct MaxEntry(Entry);

impl Eq for MaxEntry {}

impl PartialEq for MaxEntry {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl PartialOrd for MaxEntry {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MaxEntry {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    // Max-heap: natural key order, same key -> larger id first
    // 最大堆：自然 key 顺序，相同 key 时大 id 优先
    match self.0.key.cmp(&other.0.key) {
      Ordering::Equal => self.0.id.cmp(&other.0.id),
      ord => ord,
    }
  }
}

/// Forward merge iterator
/// 正向归并迭代器
pub struct MergeIter<'a> {
  sources: Vec<Option<Source>>,
  heap: BinaryHeap<MinEntry>,
  last_key: Option<Box<[u8]>>,
  _marker: PhantomData<&'a ()>,
}

/// Macro to implement the merge logic for both forward and reverse iterators
/// 用于实现正向和反向迭代器归并逻辑的宏
macro_rules! impl_merge_iter {
  ($name:ident, $wrapper:ident, $next_fn:ident) => {
    impl Iterator for $name<'_> {
      type Item = Kv;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        loop {
          let mut peek = self.heap.peek_mut()?;
          let idx = peek.0.idx;

          // Check duplicate
          // 检查重复
          let is_dup = self.last_key.as_ref().is_some_and(|k| k == &peek.0.key);

          if is_dup {
            // Duplicate: advance source
            // 重复项：推进源
            if let Some(src) = &mut self.sources[idx] {
              if let Some((k, v)) = src.iter.$next_fn() {
                peek.0.key = k.clone();
                peek.0.pos = *v;
                continue;
              }
            }
            self.sources[idx] = None;
            PeekMut::pop(peek);
            continue;
          }

          let cur_key = mem::take(&mut peek.0.key);
          let cur_pos = peek.0.pos;

          // Advance source
          // 推进源迭代器
          if let Some(src) = &mut self.sources[idx] {
            if let Some((k, v)) = src.iter.$next_fn() {
              peek.0.key = k.clone();
              peek.0.pos = *v;
            } else {
              self.sources[idx] = None;
              PeekMut::pop(peek);
            }
          } else {
            PeekMut::pop(peek);
          }

          self.last_key = Some(cur_key.clone());
          return Some((cur_key, cur_pos));
        }
      }
    }

    impl FusedIterator for $name<'_> {}
  };
}

impl<'a> MergeIter<'a> {
  pub(crate) fn new(
    mems: impl IntoIterator<Item = Mem>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let mut sources = Vec::new();
    let mut heap = BinaryHeap::new();

    for (idx, m) in mems.into_iter().enumerate() {
      let id = m.id;

      // SAFETY: Mem keeps data alive
      // 安全：Mem 保持数据存活
      let range = unsafe {
        mem::transmute::<btree_map::Range<'_, _, _>, btree_map::Range<'static, _, _>>(
          m.data.range::<[u8], _>((start, end)),
        )
      };

      let mut src = Source {
        iter: range,
        _mem: m,
      };

      if let Some((k, v)) = src.iter.next() {
        heap.push(MinEntry(Entry {
          key: k.clone(),
          pos: *v,
          id,
          idx,
        }));
        sources.push(Some(src));
      } else {
        sources.push(None);
      }
    }

    Self {
      sources,
      heap,
      last_key: None,
      _marker: PhantomData,
    }
  }
}

impl_merge_iter!(MergeIter, MinEntry, next);

/// Reverse merge iterator
/// 反向归并迭代器
pub struct MergeRevIter<'a> {
  sources: Vec<Option<Source>>,
  heap: BinaryHeap<MaxEntry>,
  last_key: Option<Box<[u8]>>,
  _marker: PhantomData<&'a ()>,
}

impl MergeRevIter<'_> {
  pub(crate) fn new(
    mems: impl IntoIterator<Item = Mem>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let mut sources = Vec::new();
    let mut heap = BinaryHeap::new();

    for (idx, m) in mems.into_iter().enumerate() {
      let id = m.id;

      // SAFETY: Mem keeps data alive
      // 安全：Mem 保持数据存活
      let range = unsafe {
        mem::transmute::<btree_map::Range<'_, _, _>, btree_map::Range<'static, _, _>>(
          m.data.range::<[u8], _>((start, end)),
        )
      };

      let mut src = Source {
        iter: range,
        _mem: m,
      };

      if let Some((k, v)) = src.iter.next_back() {
        heap.push(MaxEntry(Entry {
          key: k.clone(),
          pos: *v,
          id,
          idx,
        }));
        sources.push(Some(src));
      } else {
        sources.push(None);
      }
    }

    Self {
      sources,
      heap,
      last_key: None,
      _marker: PhantomData,
    }
  }
}

impl_merge_iter!(MergeRevIter, MaxEntry, next_back);
