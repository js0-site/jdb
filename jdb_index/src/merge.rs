//! Merge - Merge mem tables with one async stream
//! 合并 - 将内存表与一个异步流合并

use std::{
  cmp::Ordering,
  collections::BinaryHeap,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
pub use jdb_base::table::{Asc, Desc, PeekIter};
use jdb_base::table::{Kv, Order};

/// Merge stream for combining mem iterators with one async stream
/// 合并流，用于组合内存迭代器和一个异步流
pub struct Merge<I, S, O> {
  heap: BinaryHeap<I>,
  sst: S,
  /// Current sst item (buffered)
  /// 当前 sst 元素（缓冲）
  sst_cur: Option<Kv>,
  /// Last emitted key for deduplication
  /// 上一个输出的键，用于去重
  last_key: Option<Box<[u8]>>,
  /// Whether to skip tombstone entries
  /// 是否跳过删除标记
  skip_rm: bool,
  _o: std::marker::PhantomData<O>,
}

impl<I, S, O> Unpin for Merge<I, S, O> {}

impl<I: PeekIter, S: Stream<Item = Kv> + Unpin, O: Order> Merge<I, S, O> {
  #[inline]
  pub fn new(mem: Vec<I>, sst: S, skip_rm: bool) -> Self {
    let heap = BinaryHeap::from(mem);
    Self {
      heap,
      sst,
      sst_cur: None,
      last_key: None,
      skip_rm,
      _o: std::marker::PhantomData,
    }
  }

  /// Poll sst for next item
  /// 轮询 sst 获取下一个元素
  #[inline]
  fn poll_sst(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    if self.sst_cur.is_some() {
      return Poll::Ready(());
    }
    match Pin::new(&mut self.sst).poll_next(cx) {
      Poll::Ready(item) => {
        self.sst_cur = item;
        Poll::Ready(())
      }
      Poll::Pending => Poll::Pending,
    }
  }

  /// Check if key is duplicate
  /// 检查键是否重复
  #[inline]
  fn is_dup(&self, key: &[u8]) -> bool {
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }

  /// Compare mem top with sst current
  /// 比较 mem 堆顶与 sst 当前元素
  #[inline]
  fn cmp_mem_sst(mem_key: &[u8], sst: &Kv) -> Ordering {
    match O::cmp(mem_key, &sst.0) {
      // Same key: mem always wins (mem is newer than sst)
      // 相同键：mem 总是胜出（mem 比 sst 更新）
      Ordering::Equal => Ordering::Less,
      ord => ord,
    }
  }
}

impl<I: PeekIter, S: Stream<Item = Kv> + Unpin, O: Order> Stream for Merge<I, S, O> {
  type Item = Kv;

  #[inline]
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    // Ensure sst is polled
    // 确保 sst 已轮询
    if this.poll_sst(cx).is_pending() {
      return Poll::Pending;
    }

    // Merge loop
    // 合并循环
    loop {
      // Pick next item: compare mem heap top with sst current
      // 选择下一个元素：比较 mem 堆顶与 sst 当前元素
      let (key, pos, from_mem) = match (this.heap.peek(), &this.sst_cur) {
        (Some(m), Some(s)) => {
          let m_kv = m.peek().unwrap();
          match Self::cmp_mem_sst(&m_kv.0, s) {
            // mem wins (smaller key or same key)
            // mem 胜出（键更小或相同键）
            Ordering::Less | Ordering::Equal => {
              let mut iter = this.heap.pop().unwrap();
              let kv = iter.take().unwrap();
              if iter.peek().is_some() {
                this.heap.push(iter);
              }
              (kv.0, kv.1, true)
            }
            // sst wins
            // sst 胜出
            Ordering::Greater => {
              let (k, p) = this.sst_cur.take().unwrap();
              (k, p, false)
            }
          }
        }
        (Some(_), None) => {
          let mut iter = this.heap.pop().unwrap();
          let kv = iter.take().unwrap();
          if iter.peek().is_some() {
            this.heap.push(iter);
          }
          (kv.0, kv.1, true)
        }
        (None, Some(_)) => {
          let (k, p) = this.sst_cur.take().unwrap();
          (k, p, false)
        }
        (None, None) => return Poll::Ready(None),
      };

      // Dedup: skip if same as last key
      // 去重：如果与上一个键相同则跳过
      if this.is_dup(&key) {
        if !from_mem && this.poll_sst(cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Skip tombstone if requested
      // 如果需要则跳过删除标记
      if this.skip_rm && pos.is_tombstone() {
        this.last_key = Some(key);
        if !from_mem && this.poll_sst(cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Valid entry found
      // 找到有效条目
      if !from_mem && this.poll_sst(cx).is_pending() {
        this.last_key = Some(key.clone());
        return Poll::Ready(Some((key, pos)));
      }
      this.last_key = Some(key.clone());
      return Poll::Ready(Some((key, pos)));
    }
  }
}

/// Type aliases
/// 类型别名
pub type MergeAsc<I, S> = Merge<I, S, Asc>;
pub type MergeDesc<I, S> = Merge<I, S, Desc>;
