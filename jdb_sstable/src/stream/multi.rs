//! Multi-table merge stream for Read
//! Read 的多表合并流

use std::{
  cell::RefCell,
  cmp::Ordering,
  collections::BinaryHeap,
  marker::PhantomData,
  ops::Bound,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::table::Kv;
use jdb_fs::FileLru;

use super::{asc_stream, desc_stream, to_owned};
use crate::TableInfo;

/// Order trait for heap comparison
/// 堆比较的排序 trait
pub trait MultiOrder {
  /// Compare two keys for heap ordering
  /// 比较两个键的堆排序
  fn cmp(a: &[u8], b: &[u8]) -> Ordering;
}

/// Ascending order marker
/// 升序标记
pub struct Asc;

impl MultiOrder for Asc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    // Min-heap: reverse comparison
    // 最小堆：反转比较
    b.cmp(a)
  }
}

/// Descending order marker
/// 降序标记
pub struct Desc;

impl MultiOrder for Desc {
  #[inline]
  fn cmp(a: &[u8], b: &[u8]) -> Ordering {
    // Max-heap: normal comparison
    // 最大堆：正常比较
    a.cmp(b)
  }
}

/// Heap item for merge
/// 合并堆项
struct Item<O> {
  kv: Kv,
  table_id: u64,
  idx: usize,
  _o: PhantomData<O>,
}

impl<O> Item<O> {
  #[inline]
  fn new(kv: Kv, table_id: u64, idx: usize) -> Self {
    Self {
      kv,
      table_id,
      idx,
      _o: PhantomData,
    }
  }
}

impl<O> PartialEq for Item<O> {
  fn eq(&self, other: &Self) -> bool {
    self.kv.0 == other.kv.0 && self.table_id == other.table_id
  }
}

impl<O> Eq for Item<O> {}

impl<O: MultiOrder> PartialOrd for Item<O> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<O: MultiOrder> Ord for Item<O> {
  fn cmp(&self, other: &Self) -> Ordering {
    match O::cmp(&self.kv.0, &other.kv.0) {
      // Same key: higher table_id (newer) pops first
      // 相同键：table_id 大的（更新）先弹出
      Ordering::Equal => self.table_id.cmp(&other.table_id),
      ord => ord,
    }
  }
}

/// Multi-table merge stream (generic over order)
/// 多表合并流（泛型排序）
pub struct Multi<'a, O> {
  streams: Vec<Pin<Box<dyn Stream<Item = Kv> + 'a>>>,
  table_ids: Vec<u64>,
  heap: BinaryHeap<Item<O>>,
  refill_idx: Option<usize>,
  last_key: Option<Box<[u8]>>,
  init_idx: usize,
}

impl<O> Unpin for Multi<'_, O> {}

impl<'a, O: MultiOrder> Multi<'a, O> {
  fn poll_init(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    while self.init_idx < self.streams.len() {
      let idx = self.init_idx;
      match self.streams[idx].as_mut().poll_next(cx) {
        Poll::Ready(Some(kv)) => {
          self.heap.push(Item::new(kv, self.table_ids[idx], idx));
          self.init_idx += 1;
        }
        Poll::Ready(None) => self.init_idx += 1,
        Poll::Pending => return Poll::Pending,
      }
    }
    Poll::Ready(())
  }

  fn refill(&mut self, idx: usize, cx: &mut Context<'_>) -> Poll<()> {
    match self.streams[idx].as_mut().poll_next(cx) {
      Poll::Ready(Some(kv)) => {
        self.heap.push(Item::new(kv, self.table_ids[idx], idx));
        Poll::Ready(())
      }
      Poll::Ready(None) => Poll::Ready(()),
      Poll::Pending => {
        self.refill_idx = Some(idx);
        Poll::Pending
      }
    }
  }

  #[inline]
  fn is_dup(&self, key: &[u8]) -> bool {
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }
}

impl<O: MultiOrder> Stream for Multi<'_, O> {
  type Item = Kv;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    // Init phase
    // 初始化阶段
    if this.init_idx < this.streams.len() && this.poll_init(cx).is_pending() {
      return Poll::Pending;
    }

    // Refill phase
    // 填充阶段
    if let Some(idx) = this.refill_idx.take()
      && this.refill(idx, cx).is_pending()
    {
      return Poll::Pending;
    }

    // Merge loop
    // 合并循环
    loop {
      let Some(item) = this.heap.pop() else {
        return Poll::Ready(None);
      };

      let Item { kv, idx, .. } = item;

      // Dedup
      // 去重
      if this.is_dup(&kv.0) {
        if this.refill(idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      this.refill_idx = Some(idx);
      this.last_key = Some(kv.0.clone());
      return Poll::Ready(Some(kv));
    }
  }
}

/// Multi-table ascending merge stream
/// 多表升序合并流
pub type MultiAsc<'a> = Multi<'a, Asc>;

/// Multi-table descending merge stream
/// 多表降序合并流
pub type MultiDesc<'a> = Multi<'a, Desc>;

impl<'a> MultiAsc<'a> {
  pub fn new(
    tables: &'a [TableInfo],
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    let mut streams: Vec<Pin<Box<dyn Stream<Item = Kv> + 'a>>> = Vec::with_capacity(tables.len());
    let mut table_ids = Vec::with_capacity(tables.len());

    for info in tables {
      let s = asc_stream(info, Rc::clone(&lru), bound_ref(&start), bound_ref(&end));
      streams.push(Box::pin(s));
      table_ids.push(info.meta().id);
    }

    Self {
      streams,
      table_ids,
      heap: BinaryHeap::with_capacity(tables.len()),
      refill_idx: None,
      last_key: None,
      init_idx: 0,
    }
  }
}

impl<'a> MultiDesc<'a> {
  pub fn new(
    tables: &'a [TableInfo],
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    let mut streams: Vec<Pin<Box<dyn Stream<Item = Kv> + 'a>>> = Vec::with_capacity(tables.len());
    let mut table_ids = Vec::with_capacity(tables.len());

    for info in tables {
      let s = desc_stream(info, Rc::clone(&lru), bound_ref(&start), bound_ref(&end));
      streams.push(Box::pin(s));
      table_ids.push(info.meta().id);
    }

    Self {
      streams,
      table_ids,
      heap: BinaryHeap::with_capacity(tables.len()),
      refill_idx: None,
      last_key: None,
      init_idx: 0,
    }
  }
}

/// Convert owned bound to reference
/// 将所有权边界转换为引用
#[inline]
fn bound_ref(bound: &Bound<Box<[u8]>>) -> Bound<&[u8]> {
  match bound {
    Bound::Unbounded => Bound::Unbounded,
    Bound::Included(k) => Bound::Included(k.as_ref()),
    Bound::Excluded(k) => Bound::Excluded(k.as_ref()),
  }
}
