//! Merge - Merge mem tables with one async table
//! 合并 - 将内存表与一个异步表合并

use std::{
  collections::BinaryHeap,
  ops::Bound,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::table::{Kv, SsTable, Table};

pub use crate::order::{Asc, Desc};
use crate::order::{Item, Order};

/// Merge stream for combining mem iterators with one async stream
/// 合并流，用于组合内存迭代器和一个异步流
pub struct Merge<M, S, O> {
  mem: Vec<M>,
  sst: S,
  heap: BinaryHeap<Item<O>>,
  /// Index of source needing refill (Lazy Refill pattern)
  /// 需要重新填充的源索引（惰性填充模式）
  refill_idx: Option<usize>,
  /// Last emitted key for deduplication
  /// 上一个输出的键，用于去重
  last_key: Option<Box<[u8]>>,
  /// Whether to skip tombstone entries
  /// 是否跳过删除标记
  skip_rm: bool,
  /// Whether mem initialization is done
  /// mem 是否已初始化
  mem_init: bool,
  /// Whether sst initialization is done
  /// sst 是否已初始化
  sst_init: bool,
}

/// SST source index (after all mem sources)
/// SST 源索引（在所有 mem 源之后）
const SST_IDX: usize = usize::MAX;

impl<M, S, O> Unpin for Merge<M, S, O> {}

impl<M: Iterator<Item = Kv>, S: Stream<Item = Kv> + Unpin, O: Order> Merge<M, S, O> {
  #[inline]
  pub fn new(mem: Vec<M>, sst: S, skip_rm: bool) -> Self {
    let cap = mem.len() + 1;
    Self {
      mem,
      sst,
      heap: BinaryHeap::with_capacity(cap),
      refill_idx: None,
      last_key: None,
      skip_rm,
      mem_init: false,
      sst_init: false,
    }
  }

  /// Initialize mem iterators into heap
  /// 初始化内存迭代器到堆中
  #[inline]
  fn init_mem(&mut self) {
    for (i, iter) in self.mem.iter_mut().enumerate() {
      if let Some((key, pos)) = iter.next() {
        self.heap.push(Item::new(key, pos, i));
      }
    }
  }

  /// Poll sst stream for initialization
  /// 轮询 SST 流进行初始化
  fn poll_init_sst(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    match Pin::new(&mut self.sst).poll_next(cx) {
      Poll::Ready(Some((key, pos))) => {
        self.heap.push(Item::new(key, pos, SST_IDX));
        Poll::Ready(())
      }
      Poll::Ready(None) => Poll::Ready(()),
      Poll::Pending => Poll::Pending,
    }
  }

  /// Poll source by index
  /// 按索引轮询源
  #[inline]
  fn poll_src(&mut self, idx: usize, cx: &mut Context<'_>) -> Poll<Option<Kv>> {
    if idx == SST_IDX {
      Pin::new(&mut self.sst).poll_next(cx)
    } else {
      Poll::Ready(self.mem[idx].next())
    }
  }

  /// Try to refill from source, return Pending if blocked
  /// 尝试从源填充，如果阻塞则返回 Pending
  #[inline]
  fn refill(&mut self, idx: usize, cx: &mut Context<'_>) -> Poll<()> {
    match self.poll_src(idx, cx) {
      Poll::Ready(Some((key, pos))) => {
        self.heap.push(Item::new(key, pos, idx));
        Poll::Ready(())
      }
      Poll::Ready(None) => Poll::Ready(()),
      Poll::Pending => {
        self.refill_idx = Some(idx);
        Poll::Pending
      }
    }
  }

  /// Check if key is duplicate of the last emitted key
  /// 检查键是否与上一个输出的键重复
  #[inline]
  fn is_dup(&self, key: &[u8]) -> bool {
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }
}

impl<M: Iterator<Item = Kv>, S: Stream<Item = Kv> + Unpin, O: Order> Stream for Merge<M, S, O> {
  type Item = Kv;

  #[inline]
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    // 1. Initialization Phase
    // 1. 初始化阶段
    if !this.sst_init {
      if !this.mem_init {
        this.init_mem();
        this.mem_init = true;
      }
      if this.poll_init_sst(cx).is_pending() {
        return Poll::Pending;
      }
      this.sst_init = true;
    }

    // 2. Refill Phase (Resume from Pending)
    // 2. 填充阶段（从 Pending 恢复）
    if let Some(idx) = this.refill_idx.take()
      && this.refill(idx, cx).is_pending()
    {
      return Poll::Pending;
    }

    // 3. Merge Loop
    // 3. 合并循环
    loop {
      let Some(item) = this.heap.pop() else {
        return Poll::Ready(None);
      };

      let src_idx = item.src_idx;
      let Item { key, pos, .. } = item;

      // Dedup: skip if same as last key
      // 去重：如果与上一个键相同则跳过
      if this.is_dup(&key) {
        if this.refill(src_idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Skip tombstone if requested
      // 如果需要则跳过删除标记
      if this.skip_rm && pos.is_tombstone() {
        this.last_key = Some(key);
        if this.refill(src_idx, cx).is_pending() {
          return Poll::Pending;
        }
        continue;
      }

      // Valid entry found
      // 找到有效条目
      this.refill_idx = Some(src_idx);
      this.last_key = Some(key.clone());
      return Poll::Ready(Some((key, pos)));
    }
  }
}

/// Type aliases
/// 类型别名
pub type MergeAsc<M, S> = Merge<M, S, Asc>;
pub type MergeDesc<M, S> = Merge<M, S, Desc>;

/// Builder for creating merge streams from Table + SsTable
/// 从 Table + SsTable 创建合并流的构建器
pub struct MergeBuilder<'a, T: Table, A: SsTable> {
  mem: &'a [T],
  sst: &'a mut A,
  skip_rm: bool,
}

impl<'a, T: Table, A: SsTable> MergeBuilder<'a, T, A> {
  #[inline]
  pub fn new(mem: &'a [T], sst: &'a mut A, skip_rm: bool) -> Self {
    Self { mem, sst, skip_rm }
  }

  /// Create ascending range stream
  /// 创建升序范围流
  #[inline]
  pub fn range(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> MergeAsc<T::Iter<'a>, A::RangeStream<'_>> {
    let mem = self.mem.iter().map(|t| t.range(start, end)).collect();
    let sst = self.sst.range(start, end);
    Merge::new(mem, sst, self.skip_rm)
  }

  /// Create descending range stream
  /// 创建降序范围流
  #[inline]
  pub fn rev_range(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> MergeDesc<std::iter::Rev<T::Iter<'a>>, A::RevStream<'_>> {
    let mem = self.mem.iter().map(|t| t.range(start, end).rev()).collect();
    let sst = self.sst.rev_range(start, end);
    Merge::new(mem, sst, self.skip_rm)
  }

  /// Create full ascending iterator
  /// 创建完整升序迭代器
  #[inline]
  pub fn iter(&mut self) -> MergeAsc<T::Iter<'a>, A::RangeStream<'_>> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Create full descending iterator
  /// 创建完整降序迭代器
  #[inline]
  pub fn rev_iter(&mut self) -> MergeDesc<std::iter::Rev<T::Iter<'a>>, A::RevStream<'_>> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }
}
