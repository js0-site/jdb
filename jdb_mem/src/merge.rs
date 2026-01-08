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

/// Macro to define heap entry wrapper with ordering
/// 定义带排序的堆条目包装宏
macro_rules! def_entry_wrapper {
  ($name:ident, $cmp_expr:expr) => {
    struct $name(Entry);

    impl Eq for $name {}

    impl PartialEq for $name {
      #[inline]
      fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
      }
    }

    impl PartialOrd for $name {
      #[inline]
      fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
      }
    }

    impl Ord for $name {
      #[inline]
      fn cmp(&self, other: &Self) -> Ordering {
        #[allow(clippy::redundant_closure_call)]
        $cmp_expr(&self.0, &other.0)
      }
    }
  };
}

// Min-heap: reverse key order, same key -> larger id first
// 最小堆：反转 key 顺序，相同 key 时大 id 优先
def_entry_wrapper!(MinEntry, |a: &Entry, b: &Entry| {
  match b.key.cmp(&a.key) {
    Ordering::Equal => a.id.cmp(&b.id),
    ord => ord,
  }
});

// Max-heap: natural key order, same key -> larger id first
// 最大堆：自然 key 顺序，相同 key 时大 id 优先
def_entry_wrapper!(MaxEntry, |a: &Entry, b: &Entry| {
  match a.key.cmp(&b.key) {
    Ordering::Equal => a.id.cmp(&b.id),
    ord => ord,
  }
});

/// Macro to define merge iterator struct and impl
/// 定义归并迭代器结构体和实现的宏
macro_rules! def_merge_iter {
  ($(#[$meta:meta])* $name:ident, $wrapper:ident, $next_fn:ident) => {
    $(#[$meta])*
    pub struct $name<'a> {
      sources: Vec<Option<Source>>,
      heap: BinaryHeap<$wrapper>,
      last_key: Option<Box<[u8]>>,
      _marker: PhantomData<&'a ()>,
    }

    impl $name<'_> {
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

          if let Some((k, v)) = src.iter.$next_fn() {
            heap.push($wrapper(Entry {
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

          let cur_pos = peek.0.pos;

          // Advance source
          // 推进源迭代器
          if let Some(src) = &mut self.sources[idx] {
            if let Some((k, v)) = src.iter.$next_fn() {
              // Optimization: Replace key directly, returning the old one
              // 优化：直接替换 key，返回旧 key
              let cur_key = mem::replace(&mut peek.0.key, k.clone());
              peek.0.pos = *v;
              self.last_key = Some(cur_key.clone());
              return Some((cur_key, cur_pos));
            }
            self.sources[idx] = None;
          }

          // Source exhausted or missing, pop entry
          // 源耗尽或缺失，弹出条目
          let Entry { key, .. } = PeekMut::pop(peek).0;
          self.last_key = Some(key.clone());
          return Some((key, cur_pos));
        }
      }
    }

    impl FusedIterator for $name<'_> {}
  };
}

def_merge_iter!(
  /// Forward merge iterator
  /// 正向归并迭代器
  MergeIter,
  MinEntry,
  next
);

def_merge_iter!(
  /// Reverse merge iterator
  /// 反向归并迭代器
  MergeRevIter,
  MaxEntry,
  next_back
);
