use std::collections::HashSet;

use jdb_base::sst::Level;
use sorted_vec::SortedVec;

use crate::Meta;

/// Levels managing SST metadatas
/// 管理 SST 元数据的层级
#[derive(Debug)]
pub struct Levels {
  /// L0: unsorted, append-only (by insertion time)
  /// L0: 无序，按插入时间追加
  pub l0: Vec<Meta>,
  /// L1-L6: sorted by min key, disjoint
  /// L1-L6: 按 min key 排序，互不重叠
  pub levels: [SortedVec<Meta>; 6],
  pub lru: crate::Lru,
}

impl Levels {
  /// Create new Levels
  /// 创建新 Levels
  #[inline]
  pub fn new(lru: crate::Lru) -> Self {
    Self {
      l0: Vec::new(),
      levels: Default::default(),
      lru,
    }
  }

  /// Get overlapping Metas in a specific level
  /// 获取指定层级中重叠的 Meta
  pub fn overlap<K, R>(&self, level: jdb_base::sst::Level, range: R) -> Vec<Meta>
  where
    K: ?Sized + Ord,
    K: std::borrow::Borrow<[u8]>,
    R: std::ops::RangeBounds<K>,
  {
    let range = xrange::BorrowRange(&range, std::marker::PhantomData);

    match level {
      Level::L0 => self
        .l0
        .iter()
        .filter(|m| xrange::is_overlap(&range, &***m))
        .cloned()
        .collect(),
      _ => {
        // L1+ are disjoint and sorted
        // L1+ 不重叠且有序
        // SAFETY: Level is enum 0..=6. L0 is handled above. 1 <= level <= 6.
        // So 0 <= level-1 <= 5. self.levels.len() is 6.
        let vec = unsafe { self.levels.get_unchecked(level as usize - 1) };
        xrange::overlap_for_sorted(range, vec).cloned().collect()
      }
    }
  }

  /// Wrap sst::Meta and push to target level
  /// 包装 sst::Meta 并推入目标层级
  #[inline]
  pub(crate) fn push(&mut self, meta: jdb_base::ckp::Meta) {
    let wrapped = Meta::new(meta.meta, self.lru.clone());
    if meta.sst_level == Level::L0 {
      self.l0.push(wrapped);
    } else {
      // SAFETY: Level is enum 0..=6. If not L0, it is 1..=6.
      // -1 gives 0..=5. levels len is 6.
      unsafe {
        self
          .levels
          .get_unchecked_mut(meta.sst_level as usize - 1)
          .push(wrapped);
      }
    }
  }

  /// Remove metas in level with specific IDs
  /// 移除指定层级中指定 ID 的 meta
  #[inline]
  pub fn rm_ids(&mut self, level: jdb_base::sst::Level, ids: impl IntoIterator<Item = u64>) {
    let ids: HashSet<u64> = ids.into_iter().collect();
    if ids.is_empty() {
      return;
    }

    let retain_fn = |m: &Meta| {
      if ids.contains(&m.id) {
        m.mark_rm();
        false
      } else {
        true
      }
    };

    if level == Level::L0 {
      self.l0.retain(retain_fn);
    } else {
      // SAFETY:
      // Level is enum 0..=6. If not L0, it is L1..=L6.
      // Index is level - 1 => 0..=5.
      // levels.len() is 6.
      unsafe {
        self
          .levels
          .get_unchecked_mut(level as usize - 1)
          .retain(retain_fn);
      }
    }
  }

  /// Add metas in iter to levels
  /// 将 iter 中的 metas 添加到层级
  pub fn push_iter(&mut self, meta_iter: impl IntoIterator<Item = jdb_base::ckp::Meta>) {
    // Use fixed-size array instead of HashMap for better performance
    // 使用由于固定大小的数组替代 HashMap 以获得更好性能
    let mut to_add: [Vec<Meta>; Level::LEN] = Default::default();

    for m in meta_iter {
      // SAFETY:
      // m.sst_level is enum Level (0..=6).
      // Level::LEN is 7.
      // to_add len is 7.
      unsafe {
        to_add
          .get_unchecked_mut(m.sst_level as usize)
          .push(Meta::new(m.meta, self.lru.clone()));
      }
    }

    let mut add_iter = to_add.into_iter();

    // L0 (Index 0)
    // L0 (索引 0)
    if let Some(metas) = add_iter.next()
      && !metas.is_empty()
    {
      self.l0.extend(metas);
    }

    // L1-L6
    // Zip remaining adds (indices 1-6) with self.levels (indices 0-5)
    // Zip 剩余添加项 (索引 1-6) 与 self.levels (索引 0-5)
    for (metas, vec) in add_iter.zip(&mut self.levels) {
      if !metas.is_empty() {
        let mut inner = std::mem::take(vec).into_vec();
        inner.extend(metas);
        *vec = SortedVec::from_unsorted(inner);
      }
    }
  }
}
