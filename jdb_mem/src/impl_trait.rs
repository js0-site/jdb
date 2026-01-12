use core::ops::RangeBounds;
use std::borrow::Borrow;

use jdb_base::{
  Discard, Pos,
  order::{Asc, Desc},
  query::start_end,
  sst::Sst,
};

use crate::{
  Map, Mem,
  iter::{MapIter, MapRevIter, MergeIter, Merged},
};

/// Collect iterators from all map layers (now -> freeze1 -> freeze2)
/// 从所有 Map 层级收集迭代器（now -> freeze1 -> freeze2）
macro_rules! iter {
  ($self:expr, $method:ident) => {{
    let now = $self.now.$method();
    match (&$self.freeze1, &$self.freeze2) {
      (Some(m1), Some(m2)) => Merged::Three(MergeIter::new([now, m1.$method(), m2.$method()])),
      (Some(m), None) | (None, Some(m)) => Merged::Two(MergeIter::new([now, m.$method()])),
      _ => Merged::One(now),
    }
  }};

  ($self:expr, $method:ident, $bounds:expr) => {{
    let now = $self.now.$method::<[u8]>($bounds);
    match (&$self.freeze1, &$self.freeze2) {
      (Some(m1), Some(m2)) => Merged::Three(MergeIter::new([
        now,
        m1.$method::<[u8]>($bounds),
        m2.$method::<[u8]>($bounds),
      ])),
      (Some(m), None) | (None, Some(m)) => {
        Merged::Two(MergeIter::new([now, m.$method::<[u8]>($bounds)]))
      }
      _ => Merged::One(now),
    }
  }};
}

impl<F, D> jdb_base::Mem for Mem<F, D>
where
  F: Sst,
  D: Discard,
{
  /// Key type for the memory table
  /// 内存表的键类型
  type Key<'a>
    = &'a [u8]
  where
    Self: 'a;

  /// Forward iterator type
  /// 正向迭代器类型
  type Iter<'a>
    = Merged<'a, MapIter<'a>, Asc>
  where
    Self: 'a;

  /// Reverse iterator type
  /// 反向迭代器类型
  type RevIter<'a>
    = Merged<'a, MapRevIter<'a>, Desc>
  where
    Self: 'a;

  /// Insert a key-position pair into the active map with size tracking
  /// 将键值位置对插入活跃 Map，并跟踪大小
  #[inline]
  async fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    // Block if too many pending frozen maps
    // 如果待刷盘的冻结 Map 过多则阻塞
    if self.freeze1.is_some() {
      // If freeze1 is full, freeze2 must also be full (logic in rotate guarantees freeze2 fills first).
      // Wait for flush to clear freeze2, so we can move freeze1 there.
      // 如果 freeze1 满了，freeze2 肯定也满了（rotate 中的逻辑保证 freeze2 先填满）。
      // 等待刷盘清理 freeze2，这样我们就可以把 freeze1 移到那里。
      self.state.wait(&mut self.freeze2).await;
    }
    self.state.flush(&mut self.freeze2);

    let key = key.into();
    // Always add size even for overwrites: discards grows on overwrite
    // 即使是覆盖也要增加 size：覆盖时 discards 会增长，占用内存
    self.size += key.len() + Pos::SIZE + Map::ENTRY_OVERHEAD;

    // Optimize for overwrite: use get_mut to avoid tree structural changes and cloning
    // 优化覆盖操作：使用 get_mut 避免树结构变更和克隆
    if let Some(val) = self.now.inner.get_mut(&key) {
      let old_pos = *val;
      *val = pos;
      // Zero allocation: reuse the key passed in
      // 零分配：重用传入的键
      self.now.discards.push((key, old_pos));
    } else {
      self.now.inner.insert(key, pos);
    }

    if self.size >= self.rotate_size {
      self.rotate();
    }
  }

  /// Get position by key across all layers (newest first)
  /// 在所有层级中按键获取位置（由新到旧）
  #[inline]
  fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos> {
    let key = key.borrow();
    // Iterate: now -> freeze1 -> freeze2
    if let Some(pos) = self.now.get(key) {
      return Some(pos);
    }
    if let Some(map) = &self.freeze1
      && let Some(pos) = map.get(key)
    {
      return Some(pos);
    }
    if let Some(map) = &self.freeze2
      && let Some(pos) = map.get(key)
    {
      return Some(pos);
    }
    None
  }

  /// Get forward iterator over all layers
  /// 获取涵盖所有层级的正向迭代器
  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    iter!(self, iter)
  }

  /// Get reverse iterator over all layers
  /// 获取涵盖所有层级的反向迭代器
  #[inline]
  fn rev_iter(&self) -> Self::RevIter<'_> {
    iter!(self, rev_iter)
  }

  /// Get range iterator over all layers
  /// 获取涵盖所有层级的范围迭代器
  #[inline]
  fn range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::Iter<'_> {
    iter!(self, range, start_end(&range))
  }

  /// Get reverse range iterator over all layers
  /// 获取涵盖所有层级的反向范围迭代器
  #[inline]
  fn rev_range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::RevIter<'_> {
    iter!(self, rev_range, start_end(&range))
  }
}
