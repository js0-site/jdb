//! Merge stream for mem iterator + sst stream
//! 内存迭代器与 SST 流的合并流

use std::{
  cmp::Ordering,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::Kv;

type Key = Box<[u8]>;

/// Generate merge stream implementation
/// 生成合并流实现
macro_rules! impl_merge {
  ($name:ident, $cmp_first:ident, $cmp_second:ident) => {
    impl<I, S> Unpin for $name<I, S> {}

    impl<I, S> $name<I, S>
    where
      I: Iterator<Item = Kv>,
      S: Stream<Item = Kv> + Unpin,
    {
      pub fn new(mut mem: I, sst: S) -> Self {
        let mem_item = mem.next();
        Self {
          mem,
          sst,
          mem_item,
          sst_item: None,
          last_key: None,
        }
      }

      #[inline]
      fn is_dup(&self, key: &[u8]) -> bool {
        self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
      }

      fn poll_sst(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.sst_item.is_some() {
          return Poll::Ready(());
        }
        match Pin::new(&mut self.sst).poll_next(cx) {
          Poll::Ready(item) => {
            self.sst_item = item;
            Poll::Ready(())
          }
          Poll::Pending => Poll::Pending,
        }
      }
    }

    impl<I, S> Stream for $name<I, S>
    where
      I: Iterator<Item = Kv>,
      S: Stream<Item = Kv> + Unpin,
    {
      type Item = Kv;

      fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        loop {
          // Poll sst if needed
          // 如果需要，轮询 sst
          if this.sst_item.is_none() && this.poll_sst(cx).is_pending() {
            // Sst pending, but mem may have data
            // sst 挂起，但 mem 可能有数据
            if let Some(ref mem_kv) = this.mem_item {
              if !this.is_dup(&mem_kv.0) {
                let kv = this.mem_item.take().expect("checked");
                this.last_key = Some(kv.0.clone());
                this.mem_item = this.mem.next();
                return Poll::Ready(Some(kv));
              }
              this.mem_item = this.mem.next();
              continue;
            }
            return Poll::Pending;
          }

          match (&this.mem_item, &this.sst_item) {
            (None, None) => return Poll::Ready(None),
            (Some(mem_kv), None) => {
              if this.is_dup(&mem_kv.0) {
                this.mem_item = this.mem.next();
                continue;
              }
              let kv = this.mem_item.take().expect("checked");
              this.last_key = Some(kv.0.clone());
              this.mem_item = this.mem.next();
              return Poll::Ready(Some(kv));
            }
            (None, Some(sst_kv)) => {
              if this.is_dup(&sst_kv.0) {
                this.sst_item = None;
                continue;
              }
              let kv = this.sst_item.take().expect("checked");
              this.last_key = Some(kv.0.clone());
              return Poll::Ready(Some(kv));
            }
            (Some(mem_kv), Some(sst_kv)) => {
              match mem_kv.0.as_ref().cmp(sst_kv.0.as_ref()) {
                Ordering::$cmp_first => {
                  if this.is_dup(&mem_kv.0) {
                    this.mem_item = this.mem.next();
                    continue;
                  }
                  let kv = this.mem_item.take().expect("checked");
                  this.last_key = Some(kv.0.clone());
                  this.mem_item = this.mem.next();
                  return Poll::Ready(Some(kv));
                }
                Ordering::$cmp_second => {
                  if this.is_dup(&sst_kv.0) {
                    this.sst_item = None;
                    continue;
                  }
                  let kv = this.sst_item.take().expect("checked");
                  this.last_key = Some(kv.0.clone());
                  return Poll::Ready(Some(kv));
                }
                Ordering::Equal => {
                  // Mem wins (newer), skip sst
                  // 内存优先（更新），跳过 sst
                  this.sst_item = None;
                  if this.is_dup(&mem_kv.0) {
                    this.mem_item = this.mem.next();
                    continue;
                  }
                  let kv = this.mem_item.take().expect("checked");
                  this.last_key = Some(kv.0.clone());
                  this.mem_item = this.mem.next();
                  return Poll::Ready(Some(kv));
                }
              }
            }
          }
        }
      }
    }
  };
}

/// Merge ascending stream (mem iterator + sst stream)
/// 升序合并流（内存迭代器 + SST 流）
pub struct MergeAsc<I, S> {
  mem: I,
  sst: S,
  mem_item: Option<Kv>,
  sst_item: Option<Kv>,
  last_key: Option<Key>,
}

impl_merge!(MergeAsc, Less, Greater);

/// Merge descending stream (mem iterator + sst stream)
/// 降序合并流（内存迭代器 + SST 流）
pub struct MergeDesc<I, S> {
  mem: I,
  sst: S,
  mem_item: Option<Kv>,
  sst_item: Option<Kv>,
  last_key: Option<Key>,
}

impl_merge!(MergeDesc, Greater, Less);
