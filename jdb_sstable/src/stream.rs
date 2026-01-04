//! Async stream for SSTable range queries
//! SSTable 范围查询的异步流

use std::{
  ops::Bound,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::table::Kv;
use jdb_fs::FileLru;

use crate::{
  Result, TableInfo,
  block::{DataBlock, restore_key},
};

type Key = Box<[u8]>;

/// Boxed future for async block loading
/// 异步块加载的 boxed future
type LoadFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<DataBlock>> + 'a>>;

/// Check if key exceeds end bound
/// 检查键是否超出结束边界
#[inline]
fn past_end(key: &[u8], end: &Bound<Key>) -> bool {
  match end {
    Bound::Unbounded => false,
    Bound::Included(k) => key > k.as_ref(),
    Bound::Excluded(k) => key >= k.as_ref(),
  }
}

/// Check if key is before start bound
/// 检查键是否在起始边界之前
#[inline]
fn before_start(key: &[u8], start: &Bound<Key>) -> bool {
  match start {
    Bound::Unbounded => false,
    Bound::Included(k) => key < k.as_ref(),
    Bound::Excluded(k) => key <= k.as_ref(),
  }
}

/// Convert bound reference to owned
/// 将边界引用转换为所有权
#[inline]
fn to_owned(bound: Bound<&[u8]>) -> Bound<Key> {
  match bound {
    Bound::Unbounded => Bound::Unbounded,
    Bound::Included(k) => Bound::Included(k.into()),
    Bound::Excluded(k) => Bound::Excluded(k.into()),
  }
}

/// Common stream state
/// 公共流状态
struct Base<'a> {
  info: &'a TableInfo,
  file_lru: &'a mut FileLru,
  start: Bound<Key>,
  end: Bound<Key>,
  loading: Option<LoadFut<'a>>,
}

impl<'a> Base<'a> {
  #[inline]
  fn new(
    info: &'a TableInfo,
    file_lru: &'a mut FileLru,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    Self {
      info,
      file_lru,
      start: to_owned(start),
      end: to_owned(end),
      loading: None,
    }
  }

  fn start_load(&mut self, idx: usize) {
    let info = self.info;
    let ptr = self.file_lru as *mut FileLru;
    self.loading = Some(Box::pin(async move {
      // Safety: file_lru lifetime is tied to stream, no concurrent access
      // 安全：file_lru 生命周期与流绑定，无并发访问
      let lru = unsafe { &mut *ptr };
      info.read_block(idx, lru).await
    }));
  }

  fn poll_load(&mut self, cx: &mut Context<'_>) -> Poll<Option<DataBlock>> {
    let Some(fut) = &mut self.loading else {
      return Poll::Ready(None);
    };
    match fut.as_mut().poll(cx) {
      Poll::Ready(Ok(block)) => {
        self.loading = None;
        Poll::Ready(Some(block))
      }
      Poll::Ready(Err(e)) => {
        self.loading = None;
        log::warn!("load block failed: {e}");
        Poll::Ready(None)
      }
      Poll::Pending => Poll::Pending,
    }
  }
}

/// Iterator state within a block (for forward iteration)
/// 块内迭代器状态（用于正向迭代）
struct IterState {
  offset: usize,
  restart_idx: u32,
  in_interval: u16,
  buf: Vec<u8>,
  count: u32,
}

impl IterState {
  #[inline]
  fn new() -> Self {
    Self {
      offset: 0,
      restart_idx: 0,
      in_interval: 0,
      buf: Vec::with_capacity(256),
      count: 0,
    }
  }

  #[inline]
  fn reset(&mut self, offset: usize) {
    self.offset = offset;
    self.restart_idx = 0;
    self.in_interval = 0;
    self.buf.clear();
    self.count = 0;
  }

  /// Get next item from block
  /// 从块中获取下一个条目
  fn next(&mut self, block: &DataBlock) -> Option<Kv> {
    if self.count >= block.item_count {
      return None;
    }

    let is_restart = self.in_interval == 0;
    if is_restart && self.restart_idx < block.restart_count {
      self.offset = block.restart_offset(self.restart_idx) as usize;
      self.restart_idx += 1;
    }

    let data = block.data.get(..block.data_end as usize)?;
    let (new_offset, pos) = crate::block::read_entry(data, self.offset, is_restart, &mut self.buf)?;
    let key = restore_key(&block.prefix, &self.buf);

    self.offset = new_offset;
    self.in_interval += 1;
    self.count += 1;

    if self.restart_idx < block.restart_count {
      let next = block.restart_offset(self.restart_idx) as usize;
      if self.offset >= next {
        self.in_interval = 0;
      }
    }

    Some((key, pos))
  }
}

/// Ascending stream (forward iteration, O(1) memory per block)
/// 升序流（正向迭代，每块 O(1) 内存）
pub struct AscStream<'a> {
  base: Base<'a>,
  cursor: usize,
  end_idx: usize,
  block: Option<DataBlock>,
  state: IterState,
}

impl<'a> AscStream<'a> {
  pub(crate) fn new(
    info: &'a TableInfo,
    file_lru: &'a mut FileLru,
    start_idx: usize,
    end_idx: usize,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    Self {
      base: Base::new(info, file_lru, start, end),
      cursor: start_idx,
      end_idx,
      block: None,
      state: IterState::new(),
    }
  }
}

impl Stream for AscStream<'_> {
  type Item = Kv;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = self.get_mut();

    loop {
      // 1. Poll pending load
      // 轮询待处理的加载
      match this.base.poll_load(cx) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Some(block)) => {
          this.state.reset(block.entries_start as usize);
          this.block = Some(block);
        }
        Poll::Ready(None) => {}
      }

      // 2. Iterate current block
      // 遍历当前块
      if let Some(block) = &this.block {
        if let Some((key, pos)) = this.state.next(block) {
          if past_end(&key, &this.base.end) {
            return Poll::Ready(None);
          }
          if !before_start(&key, &this.base.start) {
            return Poll::Ready(Some((key, pos)));
          }
          continue;
        }
        this.block = None;
      }

      // 3. Load next block
      // 加载下一个块
      if this.cursor <= this.end_idx && this.cursor < this.base.info.block_count() {
        let idx = this.cursor;
        this.cursor += 1;
        this.base.start_load(idx);
      } else {
        return Poll::Ready(None);
      }
    }
  }
}

impl Unpin for AscStream<'_> {}

/// Descending stream (reverse iteration, O(n) memory per block)
/// 降序流（反向迭代，每块 O(n) 内存）
pub struct DescStream<'a> {
  base: Base<'a>,
  cursor: usize,
  start_idx: usize,
  items: Vec<Kv>,
  idx: usize,
}

impl<'a> DescStream<'a> {
  pub(crate) fn new(
    info: &'a TableInfo,
    file_lru: &'a mut FileLru,
    start_idx: usize,
    end_idx: usize,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Self {
    Self {
      base: Base::new(info, file_lru, start, end),
      cursor: end_idx,
      start_idx,
      items: Vec::new(),
      idx: 0,
    }
  }

  fn collect(&mut self, block: &DataBlock) {
    self.items.clear();
    self.items.reserve(block.item_count as usize);
    for kv in block.iter() {
      self.items.push(kv);
    }
    self.idx = self.items.len();
  }
}

impl Stream for DescStream<'_> {
  type Item = Kv;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = self.get_mut();

    loop {
      // 1. Poll pending load
      // 轮询待处理的加载
      match this.base.poll_load(cx) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Some(block)) => {
          this.collect(&block);
        }
        Poll::Ready(None) => {}
      }

      // 2. Iterate in reverse
      // 反向遍历
      if this.idx > 0 {
        this.idx -= 1;
        let (key, pos) = std::mem::take(&mut this.items[this.idx]);
        if before_start(&key, &this.base.start) {
          return Poll::Ready(None);
        }
        if !past_end(&key, &this.base.end) {
          return Poll::Ready(Some((key, pos)));
        }
        continue;
      }
      this.items.clear();

      // 3. Load previous block
      // 加载上一个块
      let count = this.base.info.block_count();
      if this.cursor != usize::MAX && this.cursor >= this.start_idx && this.cursor < count {
        let idx = this.cursor;
        this.cursor = this.cursor.wrapping_sub(1);
        this.base.start_load(idx);
      } else {
        return Poll::Ready(None);
      }
    }
  }
}

impl Unpin for DescStream<'_> {}
