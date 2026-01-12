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

/// Internal source for MergeIter that pre-loads the next item
/// MergeIter 的内部源，预先加载下一个条目
struct Source<'a, I> {
  /// Underlying iterator
  /// 底层迭代器
  iter: I,
  /// Pre-loaded next item
  /// 预先加载的下一个条目
  next: Option<(&'a [u8], Pos)>,
}

impl<'a, I> Source<'a, I>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Create a new Source and pre-load the first item
  /// 创建一个 Source 并预加载第一个条目
  #[inline]
  fn new(mut iter: I) -> Self {
    let next = iter.next();
    Self { iter, next }
  }

  /// Pop current item and pre-load next
  /// 弹出当前条目并预加载下一个
  #[inline(always)]
  fn pop(&mut self) -> Option<(&'a [u8], Pos)> {
    self.next.take().inspect(|_| {
      self.next = self.iter.next();
    })
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
  sources: Vec<Source<'a, I>>,
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
      sources: iters.into_iter().map(Source::new).collect(),
      _marker: PhantomData,
    }
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
      let len = self.sources.len();
      if len == 0 {
        return None;
      }
      if len == 1 {
        // SAFETY: len is 1, index 0 is safe
        return unsafe { self.sources.get_unchecked_mut(0) }.pop();
      }

      let mut best_idx = 0;
      // Find first non-empty source for initial best_key
      // 查找第一个非空源作为初始最佳键
      let mut best_key = match unsafe { self.sources.get_unchecked(0) }.next {
        Some((k, _)) => k,
        None => {
          self.prune(0);
          continue;
        }
      };

      for i in 1..len {
        // SAFETY: i is bounded by len
        let source = unsafe { self.sources.get_unchecked_mut(i) };
        if let Some((key, _)) = source.next {
          let cmp = O::cmp(key, best_key);
          if cmp == Ordering::Less {
            best_idx = i;
            best_key = key;
          } else if cmp == Ordering::Equal {
            // Shadow older version key
            // 屏蔽旧版本键
            let _ = source.pop();
          }
        } else {
          // Source exhausted during search, prune and restart
          // 搜索期间源耗尽，剪枝并重启
          self.prune(i);
          continue;
        }
      }

      // Pop from the overall best source
      // 从全局最优源弹出结果
      return unsafe { self.sources.get_unchecked_mut(best_idx).pop() };
    }
  }
}

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  #[cold]
  fn prune(&mut self, i: usize) {
    let _ = self.sources.swap_remove(i);
  }
}
