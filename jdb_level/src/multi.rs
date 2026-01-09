//! Multi-table merge stream
//! 多表合并流

use std::{
  cell::RefCell,
  cmp::Ordering,
  collections::BinaryHeap,
  marker::PhantomData,
  ops::Bound,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll},
  vec,
};

use futures_core::Stream;
use jdb_base::{
  Kv,
  sst::{Asc, Desc, Order},
};
use jdb_fs::FileLru;
use jdb_sst::{Table, asc_stream, desc_stream, to_owned};

type Key = Box<[u8]>;
type Lru = Rc<RefCell<FileLru>>;

// ============================================================================
// Heap item
// 堆项
// ============================================================================

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
    match O::cmp(&other.kv.0, &self.kv.0) {
      Ordering::Equal => self.table_id.cmp(&other.table_id),
      ord => ord,
    }
  }
}

// ============================================================================
// Multi-table merge stream
// 多表合并流
// ============================================================================

// ============================================================================
// Multi-table merge stream
// 多表合并流
// ============================================================================

/// Multi-table merge stream
/// 多表合并流
pub struct Multi<'a, O, S> {
  streams: Vec<S>,
  table_ids: Vec<u64>,
  heap: BinaryHeap<Item<O>>,
  refill_idx: Option<usize>,
  last_key: Option<Key>,
  init_idx: usize,
  _p: PhantomData<&'a ()>,
}

impl<O, S> Unpin for Multi<'_, O, S> {}

impl<O: Order, S: Stream<Item = Kv> + Unpin> Multi<'_, O, S> {
  fn poll_init(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    while self.init_idx < self.streams.len() {
      let idx = self.init_idx;
      match Pin::new(&mut self.streams[idx]).poll_next(cx) {
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
    match Pin::new(&mut self.streams[idx]).poll_next(cx) {
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

impl<O: Order, S: Stream<Item = Kv> + Unpin> Stream for Multi<'_, O, S> {
  type Item = Kv;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    if this.init_idx < this.streams.len() && this.poll_init(cx).is_pending() {
      return Poll::Pending;
    }

    if let Some(idx) = this.refill_idx.take()
      && this.refill(idx, cx).is_pending()
    {
      return Poll::Pending;
    }

    loop {
      let Some(item) = this.heap.pop() else {
        return Poll::Ready(None);
      };

      let Item { kv, idx, .. } = item;

      if this.is_dup(&kv.0) {
        if this.refill(idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      this.last_key = Some(kv.0.clone());
      this.refill_idx = Some(idx);
      return Poll::Ready(Some(kv));
    }
  }
}

// ============================================================================
// Constructors
// 构造函数
// ============================================================================

pub fn new_asc<'a>(
  tables: &'a [Table],
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<'a, Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables.iter(), lru, start, end, asc_stream)
}

pub fn new_asc_from_refs<'a>(
  tables: vec::IntoIter<&'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<'a, Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables, lru, start, end, asc_stream)
}

pub fn new_desc<'a>(
  tables: &'a [Table],
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<'a, Desc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables.iter(), lru, start, end, desc_stream)
}

pub fn new_desc_from_refs<'a>(
  tables: vec::IntoIter<&'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<'a, Desc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables, lru, start, end, desc_stream)
}

fn new_multi<'a, O: Order, S: Stream<Item = Kv> + Unpin>(
  tables: impl Iterator<Item = &'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
  make_stream: impl Fn(&'a Table, Lru, Bound<Key>, Bound<Key>) -> S,
) -> Multi<'a, O, S> {
  let tables: Vec<_> = tables.collect();
  let start = to_owned(start);
  let end = to_owned(end);
  let cap = tables.len();
  let mut streams = Vec::with_capacity(cap);
  let mut table_ids = Vec::with_capacity(cap);

  for t in tables {
    streams.push(make_stream(t, Rc::clone(&lru), start.clone(), end.clone()));
    table_ids.push(t.meta().id);
  }

  Multi {
    streams,
    table_ids,
    heap: BinaryHeap::with_capacity(cap),
    refill_idx: None,
    last_key: None,
    init_idx: 0,
    _p: PhantomData,
  }
}
