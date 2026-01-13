//! Merge iterator for two sorted streams
//! 双路有序流的归并迭代器

use std::{cmp::Ordering, marker::PhantomData};

use jdb_base::{Pos, order::Order};

/// Internal source that pre-loads the next item
/// 预加载下一个条目的内部源
struct Source<'a, I> {
  /// Underlying iterator
  /// 底层迭代器
  iter: I,
  /// Pre-loaded next item
  /// 预加载的下一个条目
  next: Option<(&'a [u8], Pos)>,
}

impl<'a, I> Source<'a, I>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[inline]
  fn new(mut iter: I) -> Self {
    let next = iter.next();
    Self { iter, next }
  }

  /// Pop and preload (no closure overhead)
  /// 弹出并预加载（无闭包开销）
  #[inline(always)]
  fn pop(&mut self) -> Option<(&'a [u8], Pos)> {
    let current = self.next;
    if current.is_some() {
      self.next = self.iter.next();
    }
    current
  }
}

/// Merged iterator for two sorted streams
/// 双路有序流的合并迭代器
pub struct Merge2Iter<'a, I, O: Order>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  a: Source<'a, I>,
  b: Source<'a, I>,
  _marker: PhantomData<O>,
}

impl<'a, I, O: Order> Merge2Iter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[inline]
  pub fn new(a: I, b: I) -> Self {
    Self {
      a: Source::new(a),
      b: Source::new(b),
      _marker: PhantomData,
    }
  }
}

impl<'a, I, O: Order> Iterator for Merge2Iter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  type Item = (&'a [u8], Pos);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match (self.a.next, self.b.next) {
      (Some((ka, _)), Some((kb, _))) => match O::cmp(ka, kb) {
        Ordering::Less => self.a.pop(),
        Ordering::Greater => self.b.pop(),
        Ordering::Equal => {
          let _ = self.b.pop();
          self.a.pop()
        }
      },
      (Some(_), None) => self.a.pop(),
      (None, Some(_)) => self.b.pop(),
      (None, None) => None,
    }
  }
}
