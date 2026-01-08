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
use jdb_base::{
  Kv,
  sst::{Asc, Desc, Order},
};
use jdb_fs::FileLru;

use super::{Key, asc_stream, bound_ref, desc_stream, to_owned};
use crate::Table;

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

impl<O: Order> PartialOrd for Item<O> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<O: Order> Ord for Item<O> {
  fn cmp(&self, other: &Self) -> Ordering {
    // Reverse for BinaryHeap (max-heap -> min-heap for Asc)
    // 反转用于 BinaryHeap（最大堆 -> Asc 的最小堆）
    match O::cmp(&other.kv.0, &self.kv.0) {
      // Same key: higher table_id (newer) pops first (reverse for heap)
      // 相同键：table_id 大的（更新）先弹出（为堆反转）
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
  last_key: Option<Key>,
  init_idx: usize,
}

impl<O> Unpin for Multi<'_, O> {}

impl<O: Order> Multi<'_, O> {
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
    // Optimized: Only check if we have a last_key (fast path for first item)
    // 优化：仅在有 last_key 时检查（首项快速路径）
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }
}

impl<O: Order> Stream for Multi<'_, O> {
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

      // Dedup: skip if same key as last
      // 去重：如果与上一个键相同则跳过
      if this.is_dup(&kv.0) {
        if this.refill(idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Set last_key before refill to avoid clone after return
      // 在 refill 前设置 last_key 以避免返回后 clone
      let key = kv.0.clone();
      this.last_key = Some(key);
      this.refill_idx = Some(idx);
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

/// Stream factory trait for creating table streams
/// 流工厂 trait，用于创建表流
trait StreamFactory<'a> {
  fn create(
    info: &'a Table,
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Pin<Box<dyn Stream<Item = Kv> + 'a>>;
}

struct AscFactory;
struct DescFactory;

impl<'a> StreamFactory<'a> for AscFactory {
  fn create(
    info: &'a Table,
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Pin<Box<dyn Stream<Item = Kv> + 'a>> {
    Box::pin(asc_stream(info, lru, start, end))
  }
}

impl<'a> StreamFactory<'a> for DescFactory {
  fn create(
    info: &'a Table,
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Pin<Box<dyn Stream<Item = Kv> + 'a>> {
    Box::pin(desc_stream(info, lru, start, end))
  }
}

/// Build Multi from table iterator
/// 从表迭代器构建 Multi
fn build_multi<'a, O, F, I>(
  tables: I,
  lru: Rc<RefCell<FileLru>>,
  start: &Bound<Key>,
  end: &Bound<Key>,
) -> Multi<'a, O>
where
  O: Order,
  F: StreamFactory<'a>,
  I: Iterator<Item = &'a Table>,
{
  let tables: Vec<_> = tables.collect();
  let cap = tables.len();
  let mut streams: Vec<Pin<Box<dyn Stream<Item = Kv> + 'a>>> = Vec::with_capacity(cap);
  let mut table_ids = Vec::with_capacity(cap);

  for info in tables {
    streams.push(F::create(
      info,
      Rc::clone(&lru),
      bound_ref(start),
      bound_ref(end),
    ));
    table_ids.push(info.meta().id);
  }

  Multi {
    streams,
    table_ids,
    heap: BinaryHeap::with_capacity(cap),
    refill_idx: None,
    last_key: None,
    init_idx: 0,
  }
}

impl<'a> MultiAsc<'a> {
  pub fn new(
    tables: &'a [Table],
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    build_multi::<Asc, AscFactory, _>(tables.iter(), lru, &start, &end)
  }

  /// Create from table references (for level-based queries)
  /// 从表引用创建（用于层级查询）
  pub fn from_refs(
    tables: Vec<&'a Table>,
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    build_multi::<Asc, AscFactory, _>(tables.into_iter(), lru, &start, &end)
  }
}

impl<'a> MultiDesc<'a> {
  pub fn new(
    tables: &'a [Table],
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    build_multi::<Desc, DescFactory, _>(tables.iter(), lru, &start, &end)
  }

  /// Create from table references (for level-based queries)
  /// 从表引用创建（用于层级查询）
  pub fn from_refs(
    tables: Vec<&'a Table>,
    lru: Rc<RefCell<FileLru>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    build_multi::<Desc, DescFactory, _>(tables.into_iter(), lru, &start, &end)
  }
}
