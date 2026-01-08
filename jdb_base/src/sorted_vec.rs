//! SortedVec - Vec that maintains sorted order on push
//! 有序 Vec - 在 push 时保持有序

use std::{
  borrow::Borrow,
  ops::{Deref, DerefMut},
};

/// Vec that maintains sorted order
/// 保持有序的 Vec
#[derive(Debug, Clone, Default)]
pub struct SortedVec<T>(Vec<T>);

impl<T> SortedVec<T> {
  #[inline]
  pub fn new() -> Self {
    Self(Vec::new())
  }

  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    Self(Vec::with_capacity(cap))
  }

  /// Push item, maintaining sorted order
  /// 推入元素，保持有序
  #[inline]
  pub fn push<F>(&mut self, item: T, cmp: F)
  where
    F: Fn(&T, &T) -> std::cmp::Ordering,
  {
    // Fast path: append if empty or greater than last
    // 快速路径：如果为空或大于最后一个则追加
    if self.0.last().is_none_or(|last| cmp(&item, last).is_gt()) {
      self.0.push(item);
    } else {
      let idx = self.0.partition_point(|x| cmp(x, &item).is_lt());
      self.0.insert(idx, item);
    }
  }

  /// Find item index by key (binary search)
  /// 按键查找元素索引（二分查找）
  #[inline]
  pub fn find<Q, K, F>(&self, key: &Q, key_fn: F) -> Option<usize>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
    F: Fn(&T) -> K,
  {
    let idx = self.0.partition_point(|x| key_fn(x).borrow() < key);
    if idx < self.0.len() && key_fn(&self.0[idx]).borrow() == key {
      Some(idx)
    } else {
      None
    }
  }

  /// Remove item by key (binary search)
  /// 按键删除元素（二分查找）
  #[inline]
  pub fn rm<Q, K, F>(&mut self, key: &Q, key_fn: F) -> Option<T>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
    F: Fn(&T) -> K,
  {
    let idx = self.0.partition_point(|x| key_fn(x).borrow() < key);
    if idx < self.0.len() && key_fn(&self.0[idx]).borrow() == key {
      Some(self.0.remove(idx))
    } else {
      None
    }
  }

  /// Drain items by indices (indices must be sorted ascending)
  /// 按索引移除元素（索引必须升序排列）
  ///
  /// Optimized for batch removal (O(N)) compared to removing one by one (O(K*N)).
  /// 针对批量移除进行优化 (O(N))，优于逐个移除 (O(K*N))。
  pub fn drain_indices(&mut self, indices: &[usize]) -> Vec<T> {
    let mut res = Vec::with_capacity(indices.len());
    let mut keep = Vec::with_capacity(self.0.len() - indices.len());
    let mut idx_iter = indices.iter().peekable();

    for (i, item) in self.0.drain(..).enumerate() {
      if idx_iter.peek() == Some(&&i) {
        idx_iter.next();
        res.push(item);
      } else {
        keep.push(item);
      }
    }
    self.0 = keep;
    res
  }

  #[inline]
  pub fn into_inner(self) -> Vec<T> {
    self.0
  }
}

impl<T> Deref for SortedVec<T> {
  type Target = Vec<T>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<T> DerefMut for SortedVec<T> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<T> From<Vec<T>> for SortedVec<T> {
  #[inline]
  fn from(v: Vec<T>) -> Self {
    Self(v)
  }
}
