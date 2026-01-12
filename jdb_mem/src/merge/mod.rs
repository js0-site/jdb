//! Merge iterator for multiple sorted streams
//! 多路有序流的归并迭代器

mod merge2;
mod merge_n;

use std::{marker::PhantomData, ops::ControlFlow};

use jdb_base::{Pos, order::Order};

/// Internal source that pre-loads the next item
/// 预加载下一个条目的内部源
pub(crate) struct Source<'a, I> {
  /// Underlying iterator
  /// 底层迭代器
  iter: I,
  /// Pre-loaded next item
  /// 预加载的下一个条目
  pub(crate) next: Option<(&'a [u8], Pos)>,
}

impl<'a, I> Source<'a, I>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[inline]
  pub(crate) fn new(mut iter: I) -> Self {
    let next = iter.next();
    Self { iter, next }
  }

  /// Pop and preload (no closure overhead)
  /// 弹出并预加载（无闭包开销）
  #[inline(always)]
  pub(crate) fn pop(&mut self) -> Option<(&'a [u8], Pos)> {
    let current = self.next;
    if current.is_some() {
      self.next = self.iter.next();
    }
    current
  }
}

/// Merged iterator for multiple sorted streams
/// 多个有序流的合并迭代器
pub struct MergeIter<'a, I, O: Order>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Internal pre-loaded sources
  /// 内部预加载源
  pub(crate) sources: Vec<Source<'a, I>>,
  /// Zero-sized marker for order direction
  /// 排序方向的零大小标记
  _marker: PhantomData<O>,
}

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[inline]
  pub fn new(iters: Vec<I>) -> Self {
    Self {
      sources: iters.into_iter().map(Source::new).collect(),
      _marker: PhantomData,
    }
  }

  /// Remove exhausted source
  /// 移除已耗尽的源
  #[cold]
  pub(crate) fn prune(&mut self, i: usize) {
    let _ = self.sources.swap_remove(i);
  }
}

impl<'a, I, O: Order> Iterator for MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  type Item = (&'a [u8], Pos);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    loop {
      match self.sources.len() {
        0 => return None,
        1 => return unsafe { self.sources.get_unchecked_mut(0) }.pop(),
        2 => match self.merge2() {
          ControlFlow::Break(v) => return v,
          ControlFlow::Continue(()) => continue,
        },
        _ => match self.merge_n() {
          ControlFlow::Break(v) => return v,
          ControlFlow::Continue(()) => continue,
        },
      }
    }
  }
}
