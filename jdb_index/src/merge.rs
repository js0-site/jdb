//! Merge iterator for Mem iterators and SST streams
//! Mem 迭代器与 SST 流的合并迭代器

use std::{
  cmp::Ordering,
  collections::BinaryHeap,
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::{
  Pos,
  table::{Asc, Desc, Kv, Order},
};

/// Peekable iterator trait
/// 可预览迭代器 trait
pub trait PeekIter: Iterator<Item = Kv> {
  /// Peek current item without consuming
  /// 预览当前项而不消费
  fn peek(&self) -> Option<&Kv>;

  /// Get source id (for dedup priority)
  /// 获取源 id（用于去重优先级）
  fn id(&self) -> u64;
}

/// Heap entry for mem iterators
/// 内存迭代器的堆条目
struct MemEntry<I, O> {
  iter: I,
  _o: PhantomData<O>,
}

impl<I: PeekIter, O> MemEntry<I, O> {
  #[inline]
  fn new(iter: I) -> Self {
    Self {
      iter,
      _o: PhantomData,
    }
  }

  #[inline]
  fn key(&self) -> Option<&[u8]> {
    self.iter.peek().map(|(k, _)| k.as_ref())
  }
}

impl<I: PeekIter, O> PartialEq for MemEntry<I, O> {
  fn eq(&self, other: &Self) -> bool {
    self.key() == other.key() && self.iter.id() == other.iter.id()
  }
}

impl<I: PeekIter, O> Eq for MemEntry<I, O> {}

impl<I: PeekIter, O: Order> PartialOrd for MemEntry<I, O> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<I: PeekIter, O: Order> Ord for MemEntry<I, O> {
  fn cmp(&self, other: &Self) -> Ordering {
    match (self.key(), other.key()) {
      (Some(k1), Some(k2)) => {
        // Reverse for BinaryHeap (max-heap -> min-heap for Asc)
        // 反转用于 BinaryHeap（最大堆 -> Asc 的最小堆）
        match O::cmp(k2, k1) {
          // Same key: higher id (newer) pops first
          // 相同键：id 大的（更新）先弹出
          Ordering::Equal => self.iter.id().cmp(&other.iter.id()),
          ord => ord,
        }
      }
      (Some(_), None) => Ordering::Greater,
      (None, Some(_)) => Ordering::Less,
      (None, None) => Ordering::Equal,
    }
  }
}

/// Merge iterator combining Mem iterators and SST stream
/// 合并 Mem 迭代器和 SST 流的迭代器
pub struct Merge<I, S, O> {
  mem_heap: BinaryHeap<MemEntry<I, O>>,
  sst: S,
  sst_cur: Option<Kv>,
  sst_id: u64,
  last_key: Option<Box<[u8]>>,
  skip_rm: bool,
  _o: PhantomData<O>,
}

impl<I, S, O> Unpin for Merge<I, S, O> {}

impl<I: PeekIter, S: Stream<Item = Kv> + Unpin, O: Order> Merge<I, S, O> {
  /// Create merge iterator
  /// 创建合并迭代器
  ///
  /// - `mem_iters`: Mem iterators (newest first)
  /// - `sst`: SST stream
  /// - `skip_rm`: Skip tombstones
  pub fn new(mem_iters: Vec<I>, sst: S, skip_rm: bool) -> Self {
    let mut mem_heap = BinaryHeap::with_capacity(mem_iters.len());
    for iter in mem_iters {
      if iter.peek().is_some() {
        mem_heap.push(MemEntry::new(iter));
      }
    }
    Self {
      mem_heap,
      sst,
      sst_cur: None,
      sst_id: 0,
      last_key: None,
      skip_rm,
      _o: PhantomData,
    }
  }

  /// Check if key is duplicate
  /// 检查键是否重复
  #[inline]
  fn is_dup(&self, key: &[u8]) -> bool {
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }

  /// Check if should skip (tombstone or dup)
  /// 检查是否应跳过（删除标记或重复）
  #[inline]
  fn should_skip(&self, key: &[u8], pos: &Pos) -> bool {
    self.is_dup(key) || (self.skip_rm && pos.is_tombstone())
  }

  /// Compare mem key with sst key
  /// 比较 mem 键与 sst 键
  #[inline]
  fn cmp_keys(mem_key: Option<&[u8]>, sst_key: Option<&[u8]>, mem_id: u64, sst_id: u64) -> Ordering {
    match (mem_key, sst_key) {
      (Some(mk), Some(sk)) => match O::cmp(mk, sk) {
        Ordering::Equal => mem_id.cmp(&sst_id).reverse(),
        ord => ord,
      },
      (Some(_), None) => Ordering::Less,
      (None, Some(_)) => Ordering::Greater,
      (None, None) => Ordering::Equal,
    }
  }
}

impl<I: PeekIter, S: Stream<Item = Kv> + Unpin, O: Order> Stream for Merge<I, S, O> {
  type Item = Kv;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    // Init sst_cur if needed
    // 如果需要，初始化 sst_cur
    if this.sst_cur.is_none() {
      match Pin::new(&mut this.sst).poll_next(cx) {
        Poll::Ready(Some(kv)) => this.sst_cur = Some(kv),
        Poll::Ready(None) => {}
        Poll::Pending => {
          // Try mem first while sst pending
          // sst pending 时先尝试 mem
          if this.mem_heap.is_empty() {
            return Poll::Pending;
          }
        }
      }
    }

    loop {
      let mem_top = this.mem_heap.peek();
      let mem_key = mem_top.and_then(|e| e.key());
      let mem_id = mem_top.map(|e| e.iter.id()).unwrap_or(0);
      let sst_key = this.sst_cur.as_ref().map(|(k, _)| k.as_ref());

      // Both exhausted
      // 两者都耗尽
      if mem_key.is_none() && sst_key.is_none() {
        return Poll::Ready(None);
      }

      let cmp = Self::cmp_keys(mem_key, sst_key, mem_id, this.sst_id);

      match cmp {
        Ordering::Less | Ordering::Equal => {
          // Take from mem
          // 从 mem 取
          let mut entry = this.mem_heap.pop().expect("checked above");
          let kv = entry.iter.next().expect("peek was Some");

          // Re-push if has more
          // 如果还有更多则重新入堆
          if entry.iter.peek().is_some() {
            this.mem_heap.push(entry);
          }

          // Skip dup from sst if equal
          // 如果相等则跳过 sst 的重复
          if cmp == Ordering::Equal {
            this.sst_cur = None;
          }

          if this.should_skip(&kv.0, &kv.1) {
            continue;
          }

          this.last_key = Some(kv.0.clone());
          return Poll::Ready(Some(kv));
        }
        Ordering::Greater => {
          // Take from sst
          // 从 sst 取
          let kv = this.sst_cur.take().expect("checked above");

          // Refill sst
          // 填充 sst
          match Pin::new(&mut this.sst).poll_next(cx) {
            Poll::Ready(opt) => this.sst_cur = opt,
            Poll::Pending => {}
          }

          if this.should_skip(&kv.0, &kv.1) {
            continue;
          }

          this.last_key = Some(kv.0.clone());
          return Poll::Ready(Some(kv));
        }
      }
    }
  }
}

/// Ascending merge iterator
/// 升序合并迭代器
pub type MergeAsc<I, S> = Merge<I, S, Asc>;

/// Descending merge iterator
/// 降序合并迭代器
pub type MergeDesc<I, S> = Merge<I, S, Desc>;
