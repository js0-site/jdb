//! Prefix scan stream with owned bounds
//! 拥有边界所有权的前缀扫描流

use std::{
  cmp::Ordering,
  mem::ManuallyDrop,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use jdb_base::Kv;

type Key = Box<[u8]>;

/// Forward prefix scan stream
/// 正向前缀扫描流
pub struct PrefixAsc<M, S> {
  end: Option<Key>,
  prefix: Key,
  mem: ManuallyDrop<M>,
  sst: ManuallyDrop<S>,
  mem_item: Option<Kv>,
  sst_item: Option<Kv>,
  last_key: Option<Key>,
}

impl<M, S> Unpin for PrefixAsc<M, S> {}

impl<M, S> Drop for PrefixAsc<M, S> {
  fn drop(&mut self) {
    // Drop iterators first, then bounds
    // 先 drop 迭代器，再 drop 边界
    unsafe {
      ManuallyDrop::drop(&mut self.sst);
      ManuallyDrop::drop(&mut self.mem);
    }
  }
}

impl<M, S> PrefixAsc<M, S>
where
  M: Iterator<Item = Kv>,
  S: Stream<Item = Kv> + Unpin,
{
  /// Create from iterators with owned bounds
  /// 从迭代器创建，拥有边界所有权
  ///
  /// # Safety
  /// Caller must ensure iterators were created with bounds pointing to `end`
  /// 调用者必须确保迭代器是用指向 `end` 的边界创建的
  pub unsafe fn from_raw(mem: M, sst: S, prefix: Key, end: Option<Key>) -> Self {
    let mut s = Self {
      end,
      prefix,
      mem: ManuallyDrop::new(mem),
      sst: ManuallyDrop::new(sst),
      mem_item: None,
      sst_item: None,
      last_key: None,
    };
    s.mem_item = s.mem.next();
    s
  }

  #[inline]
  fn is_dup(&self, key: &[u8]) -> bool {
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }

  #[inline]
  fn in_range(&self, key: &[u8]) -> bool {
    if !key.starts_with(&self.prefix) {
      return false;
    }
    match &self.end {
      Some(e) => key < e.as_ref(),
      None => true,
    }
  }

  fn poll_sst(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    if self.sst_item.is_some() {
      return Poll::Ready(());
    }
    match Pin::new(&mut *self.sst).poll_next(cx) {
      Poll::Ready(item) => {
        self.sst_item = item;
        Poll::Ready(())
      }
      Poll::Pending => Poll::Pending,
    }
  }
}

impl<M, S> Stream for PrefixAsc<M, S>
where
  M: Iterator<Item = Kv>,
  S: Stream<Item = Kv> + Unpin,
{
  type Item = Kv;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    loop {
      if this.sst_item.is_none() && this.poll_sst(cx).is_pending() {
        if let Some(ref mem_kv) = this.mem_item {
          if !this.in_range(&mem_kv.0) {
            return Poll::Ready(None);
          }
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
          if !this.in_range(&mem_kv.0) {
            return Poll::Ready(None);
          }
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
          if !this.in_range(&sst_kv.0) {
            return Poll::Ready(None);
          }
          if this.is_dup(&sst_kv.0) {
            this.sst_item = None;
            continue;
          }
          let kv = this.sst_item.take().expect("checked");
          this.last_key = Some(kv.0.clone());
          return Poll::Ready(Some(kv));
        }
        (Some(mem_kv), Some(sst_kv)) => match mem_kv.0.as_ref().cmp(sst_kv.0.as_ref()) {
          Ordering::Less => {
            if !this.in_range(&mem_kv.0) {
              return Poll::Ready(None);
            }
            if this.is_dup(&mem_kv.0) {
              this.mem_item = this.mem.next();
              continue;
            }
            let kv = this.mem_item.take().expect("checked");
            this.last_key = Some(kv.0.clone());
            this.mem_item = this.mem.next();
            return Poll::Ready(Some(kv));
          }
          Ordering::Greater => {
            if !this.in_range(&sst_kv.0) {
              return Poll::Ready(None);
            }
            if this.is_dup(&sst_kv.0) {
              this.sst_item = None;
              continue;
            }
            let kv = this.sst_item.take().expect("checked");
            this.last_key = Some(kv.0.clone());
            return Poll::Ready(Some(kv));
          }
          Ordering::Equal => {
            this.sst_item = None;
            if !this.in_range(&mem_kv.0) {
              return Poll::Ready(None);
            }
            if this.is_dup(&mem_kv.0) {
              this.mem_item = this.mem.next();
              continue;
            }
            let kv = this.mem_item.take().expect("checked");
            this.last_key = Some(kv.0.clone());
            this.mem_item = this.mem.next();
            return Poll::Ready(Some(kv));
          }
        },
      }
    }
  }
}

/// Reverse prefix scan stream
/// 反向前缀扫描流
pub struct PrefixDesc<M, S> {
  prefix: Key,
  mem: ManuallyDrop<M>,
  sst: ManuallyDrop<S>,
  mem_item: Option<Kv>,
  sst_item: Option<Kv>,
  last_key: Option<Key>,
}

impl<M, S> Unpin for PrefixDesc<M, S> {}

impl<M, S> Drop for PrefixDesc<M, S> {
  fn drop(&mut self) {
    unsafe {
      ManuallyDrop::drop(&mut self.sst);
      ManuallyDrop::drop(&mut self.mem);
    }
  }
}

impl<M, S> PrefixDesc<M, S>
where
  M: Iterator<Item = Kv>,
  S: Stream<Item = Kv> + Unpin,
{
  /// Create from iterators with owned bounds
  /// 从迭代器创建，拥有边界所有权
  ///
  /// # Safety
  /// Caller must ensure iterators were created with bounds pointing to owned data
  /// 调用者必须确保迭代器是用指向拥有数据的边界创建的
  pub unsafe fn from_raw(mem: M, sst: S, prefix: Key) -> Self {
    let mut s = Self {
      prefix,
      mem: ManuallyDrop::new(mem),
      sst: ManuallyDrop::new(sst),
      mem_item: None,
      sst_item: None,
      last_key: None,
    };
    s.mem_item = s.mem.next();
    s
  }

  #[inline]
  fn is_dup(&self, key: &[u8]) -> bool {
    self.last_key.as_ref().is_some_and(|k| k.as_ref() == key)
  }

  #[inline]
  fn in_range(&self, key: &[u8]) -> bool {
    key.starts_with(&self.prefix)
  }

  fn poll_sst(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    if self.sst_item.is_some() {
      return Poll::Ready(());
    }
    match Pin::new(&mut *self.sst).poll_next(cx) {
      Poll::Ready(item) => {
        self.sst_item = item;
        Poll::Ready(())
      }
      Poll::Pending => Poll::Pending,
    }
  }
}

impl<M, S> Stream for PrefixDesc<M, S>
where
  M: Iterator<Item = Kv>,
  S: Stream<Item = Kv> + Unpin,
{
  type Item = Kv;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = &mut *self;

    loop {
      if this.sst_item.is_none() && this.poll_sst(cx).is_pending() {
        if let Some(ref mem_kv) = this.mem_item {
          if !this.in_range(&mem_kv.0) {
            return Poll::Ready(None);
          }
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
          if !this.in_range(&mem_kv.0) {
            return Poll::Ready(None);
          }
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
          if !this.in_range(&sst_kv.0) {
            return Poll::Ready(None);
          }
          if this.is_dup(&sst_kv.0) {
            this.sst_item = None;
            continue;
          }
          let kv = this.sst_item.take().expect("checked");
          this.last_key = Some(kv.0.clone());
          return Poll::Ready(Some(kv));
        }
        (Some(mem_kv), Some(sst_kv)) => {
          // Descending: Greater comes first
          // 降序：大的先出
          match mem_kv.0.as_ref().cmp(sst_kv.0.as_ref()) {
            Ordering::Greater => {
              if !this.in_range(&mem_kv.0) {
                return Poll::Ready(None);
              }
              if this.is_dup(&mem_kv.0) {
                this.mem_item = this.mem.next();
                continue;
              }
              let kv = this.mem_item.take().expect("checked");
              this.last_key = Some(kv.0.clone());
              this.mem_item = this.mem.next();
              return Poll::Ready(Some(kv));
            }
            Ordering::Less => {
              if !this.in_range(&sst_kv.0) {
                return Poll::Ready(None);
              }
              if this.is_dup(&sst_kv.0) {
                this.sst_item = None;
                continue;
              }
              let kv = this.sst_item.take().expect("checked");
              this.last_key = Some(kv.0.clone());
              return Poll::Ready(Some(kv));
            }
            Ordering::Equal => {
              this.sst_item = None;
              if !this.in_range(&mem_kv.0) {
                return Poll::Ready(None);
              }
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
