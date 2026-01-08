//! Merge iterator for multiple sorted iterators
//! 多路有序迭代器归并

use std::{
  cmp::Ordering,
  collections::BinaryHeap,
  iter::FusedIterator,
  ops::Bound,
  rc::Rc,
};

use jdb_base::Pos;

use crate::{Handle, Kv};

/// Entry in the min-heap (forward iteration)
/// 最小堆条目（正向迭代）
struct MinEntry {
  key: Box<[u8]>,
  pos: Pos,
  id: u64,
  idx: usize,
}

impl Eq for MinEntry {}

impl PartialEq for MinEntry {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.id == other.id
  }
}

impl PartialOrd for MinEntry {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MinEntry {
  fn cmp(&self, other: &Self) -> Ordering {
    // Min-heap: reverse order, same key -> larger id first
    // 最小堆：反序，相同 key 时 id 大的优先
    match other.key.as_ref().cmp(self.key.as_ref()) {
      Ordering::Equal => self.id.cmp(&other.id),
      ord => ord,
    }
  }
}

/// Entry in the max-heap (backward iteration)
/// 最大堆条目（反向迭代）
struct MaxEntry {
  key: Box<[u8]>,
  pos: Pos,
  id: u64,
  idx: usize,
}

impl Eq for MaxEntry {}

impl PartialEq for MaxEntry {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.id == other.id
  }
}

impl PartialOrd for MaxEntry {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MaxEntry {
  fn cmp(&self, other: &Self) -> Ordering {
    // Max-heap: natural order, same key -> larger id first
    // 最大堆：自然序，相同 key 时 id 大的优先
    match self.key.as_ref().cmp(other.key.as_ref()) {
      Ordering::Equal => self.id.cmp(&other.id),
      ord => ord,
    }
  }
}

/// Forward merge iterator
/// 正向归并迭代器
pub struct MergeIter {
  #[allow(dead_code)]
  handles: Vec<Rc<Handle>>,
  fwd: Vec<std::collections::btree_map::Range<'static, Box<[u8]>, Pos>>,
  min_heap: BinaryHeap<MinEntry>,
  last_key: Option<Box<[u8]>>,
  ids: Vec<u64>,
}

impl MergeIter {
  pub fn new(handles: Vec<Rc<Handle>>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
    let n = handles.len();
    let mut fwd = Vec::with_capacity(n);
    let mut min_heap = BinaryHeap::with_capacity(n);
    let mut ids = Vec::with_capacity(n);

    for h in handles.iter() {
      let id = h.mem.id();
      ids.push(id);

      // SAFETY: handles keep data alive
      // 安全：handles 保持数据存活
      let range = unsafe {
        std::mem::transmute::<
          std::collections::btree_map::Range<'_, Box<[u8]>, Pos>,
          std::collections::btree_map::Range<'static, Box<[u8]>, Pos>,
        >(h.mem.data().range::<[u8], _>((start, end)))
      };
      fwd.push(range);
    }

    // Initialize min-heap
    // 初始化最小堆
    for (idx, iter) in fwd.iter_mut().enumerate() {
      if let Some((k, &v)) = iter.next() {
        min_heap.push(MinEntry {
          key: k.clone(),
          pos: v,
          id: ids[idx],
          idx,
        });
      }
    }

    Self {
      handles,
      fwd,
      min_heap,
      last_key: None,
      ids,
    }
  }
}

impl Iterator for MergeIter {
  type Item = Kv;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let entry = self.min_heap.pop()?;

      // Advance the iterator that produced this entry
      // 推进产生此条目的迭代器
      if let Some((k, &v)) = self.fwd[entry.idx].next() {
        self.min_heap.push(MinEntry {
          key: k.clone(),
          pos: v,
          id: self.ids[entry.idx],
          idx: entry.idx,
        });
      }

      // Skip duplicate keys
      // 跳过重复的 key
      if let Some(ref last) = self.last_key {
        if entry.key.as_ref() == last.as_ref() {
          continue;
        }
      }

      self.last_key = Some(entry.key.clone());
      return Some((entry.key, entry.pos));
    }
  }
}

impl FusedIterator for MergeIter {}

/// Reverse merge iterator
/// 反向归并迭代器
pub struct MergeRevIter {
  #[allow(dead_code)]
  handles: Vec<Rc<Handle>>,
  bwd: Vec<std::collections::btree_map::Range<'static, Box<[u8]>, Pos>>,
  max_heap: BinaryHeap<MaxEntry>,
  last_key: Option<Box<[u8]>>,
  ids: Vec<u64>,
}

impl MergeRevIter {
  pub fn new(handles: Vec<Rc<Handle>>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
    let n = handles.len();
    let mut bwd = Vec::with_capacity(n);
    let mut max_heap = BinaryHeap::with_capacity(n);
    let mut ids = Vec::with_capacity(n);

    for h in handles.iter() {
      let id = h.mem.id();
      ids.push(id);

      // SAFETY: handles keep data alive
      // 安全：handles 保持数据存活
      let range = unsafe {
        std::mem::transmute::<
          std::collections::btree_map::Range<'_, Box<[u8]>, Pos>,
          std::collections::btree_map::Range<'static, Box<[u8]>, Pos>,
        >(h.mem.data().range::<[u8], _>((start, end)))
      };
      bwd.push(range);
    }

    // Initialize max-heap
    // 初始化最大堆
    for (idx, iter) in bwd.iter_mut().enumerate() {
      if let Some((k, &v)) = iter.next_back() {
        max_heap.push(MaxEntry {
          key: k.clone(),
          pos: v,
          id: ids[idx],
          idx,
        });
      }
    }

    Self {
      handles,
      bwd,
      max_heap,
      last_key: None,
      ids,
    }
  }
}

impl Iterator for MergeRevIter {
  type Item = Kv;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let entry = self.max_heap.pop()?;

      // Advance backward
      // 反向推进
      if let Some((k, &v)) = self.bwd[entry.idx].next_back() {
        self.max_heap.push(MaxEntry {
          key: k.clone(),
          pos: v,
          id: self.ids[entry.idx],
          idx: entry.idx,
        });
      }

      // Skip duplicate keys
      // 跳过重复的 key
      if let Some(ref last) = self.last_key {
        if entry.key.as_ref() == last.as_ref() {
          continue;
        }
      }

      self.last_key = Some(entry.key.clone());
      return Some((entry.key, entry.pos));
    }
  }
}

impl FusedIterator for MergeRevIter {}
