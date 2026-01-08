//! Merge iterator for multiple sorted iterators
//! 多路有序迭代器归并

use std::{cmp::Ordering, collections::BinaryHeap, iter::FusedIterator, ops::Bound, rc::Rc};

use jdb_base::Pos;
use crate::{Handle, Kv};

/// Entry in the min-heap (forward iteration)
/// 最小堆条目（正向迭代）
///
/// Optimization: Uses &'static [u8] to avoid cloning keys during heap sift operations.
/// 优化：使用 &'static [u8] 避免在堆调整时 clone key。
struct MinEntry {
  key: &'static [u8],
  pos: Pos,
  id: u64,
  idx: usize,
}

impl Eq for MinEntry {}

impl PartialEq for MinEntry {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.id == other.id
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
    // Min-heap: reverse order, same key -> larger id first (shadowing)
    // 最小堆：反序，相同 key 时 id 大的优先（覆盖）
    match other.key.cmp(self.key) {
      Ordering::Equal => self.id.cmp(&other.id),
      ord => ord,
    }
  }
}

/// Entry in the max-heap (backward iteration)
/// 最大堆条目（反向迭代）
struct MaxEntry {
  key: &'static [u8],
  pos: Pos,
  id: u64,
  idx: usize,
}

impl Eq for MaxEntry {}

impl PartialEq for MaxEntry {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.id == other.id
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
    // Max-heap: natural order, same key -> larger id first
    // 最大堆：自然序，相同 key 时 id 大的优先
    match self.key.cmp(other.key) {
      Ordering::Equal => self.id.cmp(&other.id),
      ord => ord,
    }
  }
}

/// Forward merge iterator
/// 正向归并迭代器
pub struct MergeIter {
  // Keep handles alive to ensure raw pointers in iterators remain valid
  _handles: Vec<Rc<Handle>>,
  fwd: Vec<std::collections::btree_map::Range<'static, Box<[u8]>, Pos>>,
  min_heap: BinaryHeap<MinEntry>,
  // Optimization: use static ref instead of Box to avoid alloc during internal iteration
  // 优化：使用静态引用而非 Box，避免内部迭代时的分配
  last_key: Option<&'static [u8]>,
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

      // SAFETY: handles keep data alive. transmute used for self-referential-like lifetime.
      // 安全：handles 保持数据存活。transmute 用于类自引用生命周期。
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
        // SAFETY: The key reference is valid as long as `handles` is alive.
        // 安全：只要 `handles` 存活，key 引用就有效。
        let key_ref: &'static [u8] = unsafe { std::mem::transmute(k.as_ref()) };
        min_heap.push(MinEntry {
          key: key_ref,
          pos: v,
          id: ids[idx],
          idx,
        });
      }
    }

    Self {
      _handles: handles,
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
        let key_ref: &'static [u8] = unsafe { std::mem::transmute(k.as_ref()) };
        self.min_heap.push(MinEntry {
          key: key_ref,
          pos: v,
          id: self.ids[entry.idx],
          idx: entry.idx,
        });
      }

      // Skip duplicate keys (shadowed by newer versions)
      // 跳过重复的 key（被新版本覆盖）
      if let Some(last) = self.last_key
        && entry.key == last
      {
        continue;
      }

      self.last_key = Some(entry.key);
      // Clone only when returning to the user
      // 仅在返回给用户时 Clone
      return Some((Box::from(entry.key), entry.pos));
    }
  }
}

impl FusedIterator for MergeIter {}

/// Reverse merge iterator
/// 反向归并迭代器
pub struct MergeRevIter {
  // Keep handles alive
  _handles: Vec<Rc<Handle>>,
  bwd: Vec<std::collections::btree_map::Range<'static, Box<[u8]>, Pos>>,
  max_heap: BinaryHeap<MaxEntry>,
  last_key: Option<&'static [u8]>,
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

      // SAFETY: handles keep data alive. transmute used for self-referential-like lifetime.
      // 安全：handles 保持数据存活。transmute 用于类自引用生命周期。
      let range = unsafe {
        std::mem::transmute::<
          std::collections::btree_map::Range<'_, Box<[u8]>, Pos>,
          std::collections::btree_map::Range<'static, Box<[u8]>, Pos>,
        >(h.mem.data().range::<[u8], _>((start, end)))
      };
      bwd.push(range);
    }

    for (idx, iter) in bwd.iter_mut().enumerate() {
      if let Some((k, &v)) = iter.next_back() {
        let key_ref: &'static [u8] = unsafe { std::mem::transmute(k.as_ref()) };
        max_heap.push(MaxEntry {
          key: key_ref,
          pos: v,
          id: ids[idx],
          idx,
        });
      }
    }

    Self {
      _handles: handles,
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

      if let Some((k, &v)) = self.bwd[entry.idx].next_back() {
        let key_ref: &'static [u8] = unsafe { std::mem::transmute(k.as_ref()) };
        self.max_heap.push(MaxEntry {
          key: key_ref,
          pos: v,
          id: self.ids[entry.idx],
          idx: entry.idx,
        });
      }

      if let Some(last) = self.last_key
        && entry.key == last
      {
        continue;
      }

      self.last_key = Some(entry.key);
      return Some((Box::from(entry.key), entry.pos));
    }
  }
}

impl FusedIterator for MergeRevIter {}