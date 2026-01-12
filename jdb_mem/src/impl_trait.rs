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
  iter::{MapIter, MapRevIter, MergeIter},
};

/// Collect iterators from all map layers (now + old in reverse order)
/// 从所有 Map 层级收集迭代器（now + old 倒序）
macro_rules! iter {
  ($self:expr, $method:ident) => {{
    MergeIter::new(
      std::iter::once($self.now.$method())
        .chain($self.old.iter().rev().flatten().map(|m| m.$method())),
    )
  }};

  ($self:expr, $method:ident, $bounds:expr) => {{
    MergeIter::new(
      std::iter::once($self.now.$method::<[u8]>($bounds)).chain(
        $self
          .old
          .iter()
          .rev()
          .flatten()
          .map(|m| m.$method::<[u8]>($bounds)),
      ),
    )
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
    = MergeIter<'a, MapIter<'a>, Asc>
  where
    Self: 'a;

  /// Reverse iterator type
  /// 反向迭代器类型
  type RevIter<'a>
    = MergeIter<'a, MapRevIter<'a>, Desc>
  where
    Self: 'a;

  /// Insert a key-position pair into the active map with size tracking
  /// 将键值位置对插入活跃 Map，并跟踪大小
  #[inline]
  async fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    // Block if too many pending old maps (ensures old.len() <= 2)
    // 如果待刷盘的 old map 过多则阻塞（确保 old.len() <= 2）
    if self.old[1].is_some() {
      self.state.wait(&mut self.old).await;
    }
    self.state.flush(&mut self.old);

    let key = key.into();
    // Always add size even for overwrites: discard_li grows on overwrite
    // 即使是覆盖也要增加 size：覆盖时 discard_li 会增长，占用内存
    self.size += key.len() + Pos::SIZE + Map::ENTRY_OVERHEAD;

    // Optimize for overwrite: use get_mut to avoid tree structural changes and cloning
    // 优化覆盖操作：使用 get_mut 避免树结构变更和克隆
    if let Some(val) = self.now.inner.get_mut(&key) {
      let old_pos = *val;
      *val = pos;
      // Zero allocation: reuse the key passed in
      // 零分配：重用传入的键
      self.now.discard_li.push((key, old_pos));
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
    // Iterate: now -> old[1] -> old[0]
    std::iter::once(&self.now)
      .chain(self.old.iter().rev().flatten().map(|m| &**m))
      .find_map(|map| map.get(key))
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
