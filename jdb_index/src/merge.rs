//! Merge iterator for Mem iterator and SST stream
//! Mem 迭代器与 SST 流的合并迭代器

use std::{
  cmp::Ordering,
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::{
  Pos,
  table::{Asc, Desc, Kv, Order},
};

/// Merge stream combining Mem iterator and SST stream
/// 合并 Mem 迭代器和 SST 流的流
pub struct Merge<I, S, O> {
  mem: I,
  mem_cur: Option<Kv>,
  sst: S,
  sst_cur: Option<Kv>,
  last_key: Option<Box<[u8]>>,
  skip_rm: bool,
  _o: PhantomData<O>,
}

impl<I, S, O> Unpin for Merge<I, S, O> {}

impl<I: Iterator<Item = Kv>, S: Stream<Item = Kv> + Unpin, O: Order> Merge<I, S, O> {
  /// Create merge stream
  /// 创建合并流
  ///
  /// - `mem`: Mem iterator (already merged internally)
  /// - `sst`: SST stream
  /// - `skip_rm`: Skip tombstones
  pub fn new(mut mem: I, sst: S, skip_rm: bool) -> Self {
    let mem_cur = mem.next();
    Self {
      mem,
      mem_cur,
      sst,
      sst_cur: None,
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
}

impl<I: Iterator<Item = Kv>, S: Stream<Item = Kv> + Unpin, O: Order> Stream for Merge<I, S, O> {
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
          if this.mem_cur.is_none() {
            return Poll::Pending;
          }
        }
      }
    }

    loop {
      let mem_key = this.mem_cur.as_ref().map(|(k, _)| k.as_ref());
      let sst_key = this.sst_cur.as_ref().map(|(k, _)| k.as_ref());

      // Both exhausted
      // 两者都耗尽
      if mem_key.is_none() && sst_key.is_none() {
        return Poll::Ready(None);
      }

      // Compare keys
      // 比较键
      let cmp = match (mem_key, sst_key) {
        (Some(mk), Some(sk)) => O::cmp(mk, sk),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => unreachable!(),
      };

      match cmp {
        Ordering::Less => {
          // Take from mem
          // 从 mem 取
          let kv = this.mem_cur.take().expect("checked above");
          this.mem_cur = this.mem.next();

          if this.should_skip(&kv.0, &kv.1) {
            continue;
          }

          this.last_key = Some(kv.0.clone());
          return Poll::Ready(Some(kv));
        }
        Ordering::Equal => {
          // Same key: take from mem (newer), skip sst
          // 相同键：从 mem 取（更新），跳过 sst
          let kv = this.mem_cur.take().expect("checked above");
          this.mem_cur = this.mem.next();
          this.sst_cur = None;

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

/// Ascending merge stream
/// 升序合并流
pub type MergeAsc<I, S> = Merge<I, S, Asc>;

/// Descending merge stream
/// 降序合并流
pub type MergeDesc<I, S> = Merge<I, S, Desc>;
