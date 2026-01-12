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
  // Helper to push old maps
  // 辅助规则：推送旧 Map
  (@push_old $self:expr, $sources:expr, $map:ident => $call:expr) => {
    // Optimization: Unroll checks for fixed-size array
    // 优化：展开固定大小数组的检查
    // Safety: self.old is [Option<Rc<Map>>; 2], indices 1 and 0 are valid.
    // 安全性：self.old 是 [Option<Rc<Map>>; 2]，索引 1 和 0 是有效的。
    if let Some($map) = unsafe { $self.old.get_unchecked(1) } {
      $sources.push($call);
    }
    if let Some($map) = unsafe { $self.old.get_unchecked(0) } {
      $sources.push($call);
    }
  };

  ($self:expr, $method:ident) => {{
    let mut sources = Vec::with_capacity(3);
    sources.push($self.now.$method());
    collect_iters!(@push_old $self, sources, map => map.$method());
    MergeIter::new(sources)
  }};

  // range/rev_range variant with bounds arg
  // 带边界参数的 range/rev_range 变体
  ($self:expr, $method:ident, $bounds:expr) => {{
    let mut sources = Vec::with_capacity(3);
    sources.push($self.now.$method::<[u8]>($bounds));
    collect_iters!(@push_old $self, sources, map => map.$method::<[u8]>($bounds));
    MergeIter::new(sources)
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
    self.size += key.len() + Pos::SIZE + ENTRY_OVERHEAD;

    // Optimize for overwrite: use get_mut to avoid tree structural changes
    // 优化覆盖操作：使用 get_mut 避免树结构变更
    if let Some(val) = self.now.inner.get_mut(&key) {
      let old_pos = *val;
      *val = pos;
      // We must store the key in discard_li, so the allocation was necessary.
      // 我们必须在 discard_li 中存储键，因此分配是必要的。
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
    if let Some(pos) = self.now.get(key) {
      return Some(pos);
    }

    // Optimization: avoid iterator overhead by manually unrolling logic
    // We iterate old maps in reverse order: index 1 (newer) -> index 0 (older)
    // Safety: self.old is a fixed array of size 2, so indices 1 and 0 are valid.
    // 优化：通过手动展开逻辑来避免迭代器开销
    // 我们按倒序迭代 old map：索引 1（较新）-> 索引 0（较旧）
    // 安全性：self.old 是大小为 2 的固定数组，因此索引 1 和 0 是有效的。

    if let Some(map) = unsafe { self.old.get_unchecked(1) }
      && let Some(pos) = map.get(key)
    {
      return Some(pos);
    }

    if let Some(map) = unsafe { self.old.get_unchecked(0) }
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
