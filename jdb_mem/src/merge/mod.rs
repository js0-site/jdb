//! Merge iterator for multiple sorted streams
//! 多路有序流的归并迭代器

mod merge2;
mod merge3;

use std::{cmp::Ordering, marker::PhantomData, ops::ControlFlow};

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
pub trait Merge {
  fn merge<'a, I, O: Order, const N: usize>(
    iter: &mut MergeIter<'a, I, O, N, Self>,
  ) -> Option<(&'a [u8], Pos)>
  where
    I: Iterator<Item = (&'a [u8], Pos)>,
    Self: Sized;
}

pub struct Two;
pub struct Three;

/// Merged iterator for multiple sorted streams
/// 多个有序流的合并迭代器
pub struct MergeIter<'a, I, O: Order, const N: usize = 3, M: Merge = Three>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Internal pre-loaded sources
  /// 内部预加载源
  pub(crate) sources: [Option<Source<'a, I>>; N],
  /// Current number of valid sources
  /// 当前有效源的数量
  pub(crate) len: usize,
  /// Zero-sized marker for order direction
  /// 排序方向的零大小标记
  _marker: PhantomData<(O, M)>,
}

impl<'a, I, O: Order, const N: usize, M: Merge> MergeIter<'a, I, O, N, M>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[inline]
  pub fn new(iters: impl IntoIterator<Item = I>) -> Self {
    let mut sources: [Option<Source<'a, I>>; N] = std::array::from_fn(|_| None);
    let mut len = 0;
    for iter in iters {
      if len >= N {
        panic!("Merging more than {} sources is not supported", N);
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

  /// One-source fast path
  /// 单源快速路径
  #[inline(always)]
  pub(crate) unsafe fn merge1_step(&mut self) -> ControlFlow<Option<(&'a [u8], Pos)>> {
    // Safety: Caller must ensure valid state. Inner ops require unsafe block.
    // 安全性：调用者必须确保有效状态。内部操作需要 unsafe 块。
    unsafe {
      let s = self
        .sources
        .get_unchecked_mut(0)
        .as_mut()
        .unwrap_unchecked();
      if s.next.is_none() {
        self.prune(0);
        ControlFlow::Continue(())
      } else {
        ControlFlow::Break(s.pop())
      }
    }
  }

  /// Two-source fast path
  /// 两源快速路径
  #[inline(always)]
  pub(crate) unsafe fn merge2_step(&mut self) -> ControlFlow<Option<(&'a [u8], Pos)>> {
    // Safety: Caller must ensure valid state. Inner ops require unsafe block.
    // 安全性：调用者必须确保有效状态。内部操作需要 unsafe 块。
    unsafe {
      let ptr = self.sources.as_mut_ptr();
      let s0 = (&mut *ptr).as_mut().unwrap_unchecked();
      if s0.next.is_none() {
        self.prune(0);
        return ControlFlow::Continue(());
      }

      let s1 = (&mut *ptr.add(1)).as_mut().unwrap_unchecked();
      if s1.next.is_none() {
        self.prune(1);
        return ControlFlow::Continue(());
      }

      let (k0, k1) = (s0.next.unwrap_unchecked().0, s1.next.unwrap_unchecked().0);

      ControlFlow::Break(match O::cmp(k0, k1) {
        Ordering::Less => s0.pop(),
        Ordering::Greater => s1.pop(),
        Ordering::Equal => {
          let _ = s1.pop();
          s0.pop()
        }
      })
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

impl<'a, I, O: Order, const N: usize, M: Merge> Iterator for MergeIter<'a, I, O, N, M>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  type Item = (&'a [u8], Pos);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    M::merge(self)
  }
}
