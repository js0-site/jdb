//! Merge iterator for multiple sorted streams
//! 多路有序流的归并迭代器

mod merge2;
mod merge3;

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
  /// Internal pre-loaded sources (fixed size 3)
  /// 内部预加载源（固定大小 3）
  pub(crate) sources: [Option<Source<'a, I>>; 3],
  /// Current number of valid sources
  /// 当前有效源的数量
  pub(crate) len: usize,
  /// Zero-sized marker for order direction
  /// 排序方向的零大小标记
  _marker: PhantomData<O>,
}

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[inline]
  pub fn new(iters: impl IntoIterator<Item = I>) -> Self {
    let mut sources = [None, None, None];
    let mut len = 0;
    for iter in iters {
      if len >= 3 {
        // Should not happen with current logic
        // 按当前逻辑不应发生
        unreachable!("Merging more than 3 sources is not supported");
      }
      sources[len] = Some(Source::new(iter));
      len += 1;
    }
    Self {
      sources,
      len,
      _marker: PhantomData,
    }
  }

  /// Remove exhausted source
  /// 移除已耗尽的源
  #[cold]
  pub(crate) fn prune(&mut self, i: usize) {
    if i >= self.len {
      return;
    }
    // Shift elements left to fill the gap
    // 向左移动元素以填补空缺
    for j in i..self.len - 1 {
      self.sources[j] = self.sources[j + 1].take();
    }
    // Clear the last element (now moved or originally empty)
    // 清除最后一个元素（已移动或原本为空）
    self.sources[self.len - 1] = None;
    self.len -= 1;
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
      match self.len {
        0 => return None,
        // Safety: len == 1 guarantees sources[0] is Some.
        // 安全性：len == 1 保证 sources[0] 为 Some。
        1 => {
          return unsafe {
            self
              .sources
              .get_unchecked_mut(0)
              .as_mut()
              .unwrap_unchecked()
          }
          .pop();
        }
        2 => match self.merge2() {
          ControlFlow::Break(v) => return v,
          ControlFlow::Continue(()) => continue,
        },
        _ => match self.merge3() {
          ControlFlow::Break(v) => return v,
          ControlFlow::Continue(()) => continue,
        },
      }
    }
  }
}
