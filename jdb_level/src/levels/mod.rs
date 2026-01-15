use jdb_base::sst::Level;
use sorted_vec::SortedVec;
mod update;
use crate::{LEVEL_LEN_MINUS_1, Meta, sink::Score};

mod r#impl;

/// Levels managing SST metadata
/// 管理 SST 元数据的层级
#[derive(Debug)]
pub struct Levels {
  /// L0: append-only (by insertion time)
  /// L0: 按插入时间追加
  pub l0: Vec<Meta>,
  /// L1-L6: sorted by min key, disjoint
  /// L1-L6: 按 min key 排序，互不重叠
  pub levels: [SortedVec<Meta>; LEVEL_LEN_MINUS_1],
  pub lru: crate::Lru,
  /// Scoring and GC state
  /// 评分和 GC 状态
  pub sink: Score,
}

impl Levels {
  /// Create new Levels
  /// 创建新 Levels
  #[inline]
  pub fn new(
    lru: crate::Lru,
    meta_iter: impl IntoIterator<Item = jdb_base::ckp::sst::Meta>,
  ) -> Self {
    let mut levels = Self {
      l0: Vec::new(),
      levels: Default::default(),
      lru,
      sink: Score::new([]),
    };

    levels.push_iter(meta_iter);
    levels
  }

  /// Get overlapping Metas in L0
  /// 获取 L0 中重叠的 Meta
  #[inline]
  pub fn overlap_l0<'a, K, R>(&'a self, range: &'a R) -> impl Iterator<Item = Meta> + 'a
  where
    K: ?Sized + Ord + 'a,
    K: std::borrow::Borrow<[u8]>,
    R: std::ops::RangeBounds<K> + 'a,
  {
    let r = xrange::BorrowRange(range, std::marker::PhantomData);
    self
      .l0
      .iter()
      .filter(move |m| xrange::is_overlap(&r, &***m))
      .cloned()
  }

  /// Get overlapping Metas in L1-L6
  /// 获取 L1-L6 中重叠的 Meta
  #[inline]
  pub fn overlap<'a, K, R>(&'a self, level: Level, range: &'a R) -> &'a [Meta]
  where
    K: ?Sized + Ord + 'a,
    K: std::borrow::Borrow<[u8]>,
    R: std::ops::RangeBounds<K> + 'a,
  {
    let r = xrange::BorrowRange(range, std::marker::PhantomData);
    // SAFETY: level 1-6 maps to index 0-5
    // 安全：level 1-6 对应索引 0-5
    let v = unsafe { self.levels.get_unchecked(level as usize - 1) };
    xrange::overlap_for_sorted(r, v)
  }
}
