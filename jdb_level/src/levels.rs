use std::collections::HashSet;

use jdb_base::sst::Level;
use sorted_vec::SortedVec;

use crate::{Meta, sink::Score};

/// Levels managing SST metadata
/// 管理 SST 元数据的层级
#[derive(Debug)]
pub struct Levels {
  /// L0: append-only (by insertion time)
  /// L0: 按插入时间追加
  pub l0: Vec<Meta>,
  /// L1-L6: sorted by min key, disjoint
  /// L1-L6: 按 min key 排序，互不重叠
  pub levels: [SortedVec<Meta>; 6],
  pub lru: crate::Lru,
  /// Scoring and GC state
  /// 评分和 GC 状态
  pub sink_score: Score,
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
      sink_score: Score::new([]),
    }
  }

  /// Get overlapping Metas in a level
  /// 获取层级中重叠的 Meta
  pub fn overlap<'a, K, R>(
    &'a self,
    level: Level,
    range: &'a R,
  ) -> Box<dyn Iterator<Item = Meta> + 'a>
  where
    K: ?Sized + Ord + 'a,
    K: std::borrow::Borrow<[u8]>,
    R: std::ops::RangeBounds<K> + 'a,
  {
    let r = xrange::BorrowRange(range, std::marker::PhantomData);

    match level {
      Level::L0 => Box::new(
        self
          .l0
          .iter()
          .filter(move |m| xrange::is_overlap(&r, &***m))
          .cloned(),
      ),
      _ => {
        // SAFETY: level 1-6 maps to index 0-5
        // 安全：level 1-6 对应索引 0-5
        let v = unsafe { self.levels.get_unchecked(level as usize - 1) };
        Box::new(xrange::overlap_for_sorted(r, v).cloned())
      }
    }
  }

  /// Push metadata to level
  /// 将元数据推入层级
  #[inline]
  pub(crate) fn push(&mut self, m: jdb_base::ckp::Meta) {
    let meta = Meta::new(m.meta.clone(), self.lru.clone());
    if m.sst.level == Level::L0 {
      self.l0.push(meta);
    } else {
      // SAFETY: Level 1-6 maps to index 0-5
      // 安全：Level 1-6 对应索引 0-5
      unsafe {
        self
          .levels
          .get_unchecked_mut(m.sst.level as usize - 1)
          .push(meta);
      }
    }
  }

  /// Remove metadata by IDs
  /// 通过 ID 移除元数据
  #[inline]
  pub fn rm_ids(&mut self, level: Level, ids: impl IntoIterator<Item = u64>) {
    let set: HashSet<u64> = ids.into_iter().collect();
    if set.is_empty() {
      return;
    }

    let f = |m: &Meta| {
      if set.contains(&m.id) {
        m.mark_rm();
        false
      } else {
        true
      }
    };

    if level == Level::L0 {
      self.l0.retain(f);
    } else {
      // SAFETY: Level 1-6 maps to index 0-5
      // 安全：Level 1-6 对应索引 0-5
      unsafe {
        self.levels.get_unchecked_mut(level as usize - 1).retain(f);
      }
    }
  }

  /// Push multiple metadatas (optimized)
  /// 批量推入元数据（优化版）
  pub fn push_iter(&mut self, iter: impl IntoIterator<Item = jdb_base::ckp::Meta>) {
    let mut batch: [Vec<Meta>; Level::LEN] = Default::default();

    for m in iter {
      // SAFETY: Level::LEN is 7, m.sst.level is in 0..=6
      // 安全：Level::LEN 为 7，m.sst.level 在 0..=6 范围内
      unsafe {
        batch
          .get_unchecked_mut(m.sst.level as usize)
          .push(Meta::new(m.meta.clone(), self.lru.clone()));
      }
    }

    let mut it = batch.into_iter();

    // L0
    if let Some(v) = it.next()
      && !v.is_empty()
    {
      self.l0.extend(v);
    }

    // L1-L6
    for (v, vec) in it.zip(&mut self.levels) {
      if !v.is_empty() {
        let mut inner = std::mem::take(vec).into_vec();
        inner.extend(v);
        *vec = SortedVec::from_unsorted(inner);
      }
    }
  }
}
