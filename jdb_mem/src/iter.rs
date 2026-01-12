use std::{cmp::Ordering, collections::btree_map, marker::PhantomData};

use jdb_base::{Pos, order::Order};

macro_rules! map_iter {
  ($name:ident, $inner:ty) => {
    pub struct $name<'a>(pub $inner);

    impl<'a> Iterator for $name<'a> {
      type Item = (&'a [u8], Pos);

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.as_ref(), *v))
      }
    }
  };
}

map_iter!(MapIter, btree_map::Range<'a, Box<[u8]>, Pos>);
map_iter!(
  MapRevIter,
  std::iter::Rev<btree_map::Range<'a, Box<[u8]>, Pos>>
);

/// Merged iterator for multiple sorted streams
/// 多个有序流的合并迭代器
pub struct MergeIter<'a, I, O: Order>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Internal peekable iterators
  /// 内部可预览迭代器
  pub iters: Vec<std::iter::Peekable<I>>,
  /// Zero-sized type marker for scanner direction
  /// 扫描方向的零大小类型标记
  _marker: PhantomData<O>,
}

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Create new merged iterator
  /// 创建新的合并迭代器
  #[inline]
  pub fn new(iters: Vec<I>) -> Self {
    Self {
      iters: iters.into_iter().map(|i| i.peekable()).collect(),
      _marker: PhantomData,
    }
  }
}

impl<'a, I, O: Order> Iterator for MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  type Item = (&'a [u8], Pos);

  fn next(&mut self) -> Option<Self::Item> {
    let mut best_idx = None;
    let mut best_val: Option<(&'a [u8], Pos)> = None;

    // Find the best iterator among all active ones
    // 在所有活动迭代器中找到最优的一个
    for (i, iter) in self.iters.iter_mut().enumerate() {
      if let Some(&peeked) = iter.peek() {
        if let Some((bk, bp)) = best_val {
          let cmp = O::cmp(peeked.0, bk);
          // If current key is "smaller" (per Order) or keys equal but higher version
          // 如果当前键更“小”（按 Order 策略）或键相等但版本更高
          if cmp == Ordering::Less || (cmp == Ordering::Equal && peeked.1.ver() > bp.ver()) {
            best_idx = Some(i);
            best_val = Some(peeked);
          }
        } else {
          best_idx = Some(i);
          best_val = Some(peeked);
        }
      }
    }

    if let Some(bi) = best_idx {
      // Advance the chosen iterator and get the result
      // 推进选定的迭代器并获取结果
      // SAFETY: best_idx is Some, so this iterator must have next()
      // 安全：best_idx 为 Some，因此该迭代器肯定有 next()
      let result = unsafe { self.iters.get_unchecked_mut(bi).next().unwrap_unchecked() };
      let target_key = result.0;

      // Advance all other iters that have the same key to skip shadowed versions
      // 推进所有其他具有相同 key 的迭代器以跳过被遮蔽的版本
      for (i, iter) in self.iters.iter_mut().enumerate() {
        if i != bi && iter.peek().is_some_and(|p| p.0 == target_key) {
          unsafe { iter.next().unwrap_unchecked() };
        }
      }
      Some(result)
    } else {
      None
    }
  }
}

pub use jdb_base::order::{Asc, Desc};
