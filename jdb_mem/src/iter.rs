use jdb_base::Pos;
use std::collections::btree_map;

/// Map iterator wrapper
/// Map 迭代器封装
pub struct MapIter<'a>(pub btree_map::Range<'a, Box<[u8]>, Pos>);

impl<'a> Iterator for MapIter<'a> {
  type Item = (&'a [u8], Pos);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.0.next().map(|(k, v)| (k.as_ref(), *v))
  }
}

/// Map reverse iterator wrapper
/// Map 反向迭代器封装
pub struct MapRevIter<'a>(pub std::iter::Rev<btree_map::Range<'a, Box<[u8]>, Pos>>);

impl<'a> Iterator for MapRevIter<'a> {
  type Item = (&'a [u8], Pos);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.0.next().map(|(k, v)| (k.as_ref(), *v))
  }
}

/// Merged iterator for multiple sorted streams
/// 多个有序流的合并迭代器
pub struct MergeIter<'a, I>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Internal peekable iterators
  /// 内部可预览迭代器
  pub iters: Vec<std::iter::Peekable<I>>,
  /// Scan direction (true for reverse)
  /// 扫描方向（true 为反向）
  pub rev: bool,
}

impl<'a, I> MergeIter<'a, I>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Create new merged iterator
  /// 创建新的合并迭代器
  pub fn new(iters: Vec<I>, rev: bool) -> Self {
    Self {
      iters: iters.into_iter().map(|i| i.peekable()).collect(),
      rev,
    }
  }
}

impl<'a, I> Iterator for MergeIter<'a, I>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  type Item = (&'a [u8], Pos);

  fn next(&mut self) -> Option<Self::Item> {
    let mut best_idx: Option<usize> = None;

    // Use a simple loop for small number of iterators
    // 对于少量迭代器使用简单循环
    for i in 0..self.iters.len() {
      let target_key = if let Some(peeked) = self.iters[i].peek() {
        peeked.0
      } else {
        continue;
      };

      if let Some(bi) = best_idx {
        // SAFETY: bi is guaranteed to be a valid index and have a peeked value
        // 安全：bi 保证是有效索引且具有 peek 值
        let best_key = unsafe { self.iters.get_unchecked_mut(bi).peek().unwrap_unchecked().0 };
        let cmp = target_key.cmp(best_key);
        if (self.rev && cmp == std::cmp::Ordering::Greater)
          || (!self.rev && cmp == std::cmp::Ordering::Less)
        {
          best_idx = Some(i);
        }
      } else {
        best_idx = Some(i);
      }
    }

    if let Some(bi) = best_idx {
      // SAFETY: bi is valid and has current value
      // 安全：bi 有效且具有当前值
      let target_key = unsafe { self.iters.get_unchecked_mut(bi).peek().unwrap_unchecked().0 };
      let result = unsafe { self.iters.get_unchecked_mut(bi).next().unwrap_unchecked() };

      // Advance all other iters that have the same key to shadowing previous versions
      // 推进所有具有相同 key 的其他迭代器以屏蔽旧版本
      for i in 0..self.iters.len() {
        if i == bi {
          continue;
        }
        if unsafe {
          self
            .iters
            .get_unchecked_mut(i)
            .peek()
            .is_some_and(|p| p.0 == target_key)
        } {
          unsafe { self.iters.get_unchecked_mut(i).next().unwrap_unchecked() };
        }
      }
      Some(result)
    } else {
      None
    }
  }
}
