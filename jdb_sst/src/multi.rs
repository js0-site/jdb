//! Multi-table merge stream
//! 多表合并流

use std::{
  cmp::Ordering,
  collections::BinaryHeap,
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::{Kv, sst::OnDiscard};

use crate::Order;

/// Heap item for merge
/// 合并堆项
pub struct Item<O> {
  pub kv: Kv,
  pub src_id: u64,
  pub idx: usize,
  _o: PhantomData<O>,
}

impl<O> Item<O> {
  #[inline]
  pub fn new(kv: Kv, src_id: u64, idx: usize) -> Self {
    Self {
      kv,
      src_id,
      idx,
      _o: PhantomData,
    }
  }
}

impl<O> PartialEq for Item<O> {
  fn eq(&self, other: &Self) -> bool {
    self.kv.0 == other.kv.0 && self.src_id == other.src_id
  }
}

impl<O> Eq for Item<O> {}

impl<O: Order> PartialOrd for Item<O> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<O: Order> Ord for Item<O> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    // Reverse order for min-heap behavior
    // 反转顺序以实现最小堆行为
    match O::cmp(&other.kv.0, &self.kv.0) {
      // When keys equal, prefer higher ver (newer version)
      // key 相等时，优先 ver 更大的（更新的版本）
      Ordering::Equal => other.kv.1.ver().cmp(&self.kv.1.ver()),
      ord => ord,
    }
  }
}

/// Multi-source merge stream with discard callback
/// 带丢弃回调的多源合并流
pub struct Multi<O, S, D> {
  streams: Vec<S>,
  src_ids: Vec<u64>,
  heap: BinaryHeap<Item<O>>,
  refill_idx: Option<usize>,
  last_key: Option<Box<[u8]>>,
  init_idx: usize,
  discard: D,
  bottom: bool,
}

impl<O, S, D> Unpin for Multi<O, S, D> {}

impl<O: Order, S, D> Multi<O, S, D> {
  pub fn new(streams: Vec<S>, src_ids: Vec<u64>, discard: D, bottom: bool) -> Self {
    let cap = streams.len();
    Self {
      streams,
      src_ids,
      heap: BinaryHeap::with_capacity(cap),
      refill_idx: None,
      last_key: None,
      init_idx: 0,
      discard,
      bottom,
    }
  }
}

impl<O: Order, S: Stream<Item = Kv> + Unpin, D: OnDiscard> Multi<O, S, D> {
  fn poll_init(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    while self.init_idx < self.streams.len() {
      let idx = self.init_idx;
      match Pin::new(&mut self.streams[idx]).poll_next(cx) {
        Poll::Ready(Some(kv)) => {
          self.heap.push(Item::new(kv, self.src_ids[idx], idx));
          self.init_idx += 1;
        }
        Poll::Ready(None) => self.init_idx += 1,
        Poll::Pending => return Poll::Pending,
      }
    }
    Poll::Ready(())
  }

  #[inline]
  fn refill(&mut self, idx: usize, cx: &mut Context<'_>) -> Poll<()> {
    match Pin::new(&mut self.streams[idx]).poll_next(cx) {
      Poll::Ready(Some(kv)) => {
        self.heap.push(Item::new(kv, self.src_ids[idx], idx));
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

impl<O: Order, S: Stream<Item = Kv> + Unpin, D: OnDiscard> Stream for Multi<O, S, D> {
  type Item = Kv;

  #[inline]
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    // Ensure initialization
    // 确保初始化
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

      // Discard old version (duplicate key)
      // 丢弃老版本（重复 key）。Heap 保证了版本新旧顺序。
      if this.is_dup(&kv.0) {
        this.discard.discard(&kv.0, &kv.1);
        if this.refill(idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Discard tombstone at bottom level
      // 最底层丢弃墓碑
      if this.bottom && kv.1.is_tombstone() {
        this.discard.discard(&kv.0, &kv.1);
        // Must record last_key to filter out older versions of this tombstone
        // 必须记录 last_key 以过滤掉该墓碑的更早版本
        this.last_key = Some(kv.0);
        // Fix: Must refill immediately, otherwise heap is stale
        // 修复：必须立即重填，否则堆数据陈旧导致归并顺序错误
        if this.refill(idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Avoid clone: swap key out, return cloned, store original
      // 避免 clone：交换 key，返回克隆，存储原始
      let (key, pos) = kv;
      let ret_key = key.clone();
      this.last_key = Some(key);
      this.refill_idx = Some(idx);
      return Poll::Ready(Some((ret_key, pos)));
    }
  }
}
