use core::ops::RangeBounds;
use std::borrow::Borrow;

use jdb_base::{
  Discard, Pos,
  order::{Asc, Desc},
  query::start_end,
  sst::Sst,
};

use crate::{
  Mem,
  iter::{MapIter, MapRevIter, MergeIter},
};

/// Fixed overhead for each entry in the Map.
/// Estimated based on BTreeMap node overhead: node allocation (approx 1.5 - 2 pointers per entry) + node metadata.
/// 每个 Map 条目的固定开销。
/// 估计基于 BTreeMap 节点开销：节点分配（每个条目约 1.5 - 2 个指针）+ 节点元数据。
pub const ENTRY_OVERHEAD: usize = 32;

/// Collect iterators from all map layers (now + old in reverse order)
/// 从所有 Map 层级收集迭代器（now + old 倒序）
macro_rules! collect_iters {
  ($self:expr, $method:ident) => {{
    MergeIter::new(
      std::iter::once(&$self.now)
        .chain($self.old.iter().rev().map(|m| &**m))
        .map(|map| map.$method()),
    )
  }};
  // range/rev_range variant with bounds arg
  // 带边界参数的 range/rev_range 变体
  ($self:expr, $method:ident, $bounds:expr) => {{
    MergeIter::new(
      std::iter::once(&$self.now)
        .chain($self.old.iter().rev().map(|m| &**m))
        .map(|map| map.$method::<[u8]>($bounds)),
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
  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.state.flush(&mut self.old);
    let key = key.into();
    self.size += key.len() + Pos::SIZE + ENTRY_OVERHEAD;

    // Optimize for overwrite: use get_mut to avoid tree structural changes
    // 优化覆盖操作：使用 get_mut 避免树结构变更
    if let Some(val) = self.now.inner.get_mut(&key) {
      let old_pos = *val;
      *val = pos;
      // Discarded entry still occupies memory until sst
      // 丢弃的条目在刷盘前仍占用内存
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
    self
      .now
      .get(key)
      .or_else(|| {
        self
          .old
          .iter()
          .rev()
          .map(|m| &**m)
          .find_map(|map| map.get(key))
      })
  }

  /// Get forward iterator over all layers
  /// 获取涵盖所有层级的正向迭代器
  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    collect_iters!(self, iter)
  }

  /// Get reverse iterator over all layers
  /// 获取涵盖所有层级的反向迭代器
  #[inline]
  fn rev_iter(&self) -> Self::RevIter<'_> {
    collect_iters!(self, rev_iter)
  }

  /// Get range iterator over all layers
  /// 获取涵盖所有层级的范围迭代器
  #[inline]
  fn range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::Iter<'_> {
    let bounds = start_end(&range);
    collect_iters!(self, range, bounds)
  }

  /// Get reverse range iterator over all layers
  /// 获取涵盖所有层级的反向范围迭代器
  #[inline]
  fn rev_range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::RevIter<'_> {
    let bounds = start_end(&range);
    collect_iters!(self, rev_range, bounds)
  }
}
