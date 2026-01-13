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
  pub fn overlap<R>(&self, level: usize, range: R) -> Vec<Meta>
  where
    R: std::ops::RangeBounds<[u8]>,
  {
    if level == 0 {
      // L0 is unordered (on max key), must scan all
      // L0 (Max Key) 无序，必须全部扫描
      self
        .l0
        .iter()
        .filter(|m| xrange::is_overlap(&range, &***m))
        .cloned()
        .collect()
    } else if let Some(vec) = self.levels.get(level - 1) {
      // L1+ are disjoint and sorted
      // L1+ 不重叠且有序
      xrange::overlap_for_sorted::<_, [u8], Meta, _>(range, vec)
        .cloned()
        .collect()
    } else {
      Vec::new()
    }
  }

  /// Wrap sst::Meta and push to target level
  /// 包装 sst::Meta 并推入目标层级
  #[inline]
  pub(crate) fn push(&mut self, meta: &jdb_base::sst::Meta, level: u8) {
    let wrapped = Meta::new(meta.clone(), self.lru.clone());
    if level == 0 {
      self.l0.push(wrapped);
    } else if let Some(vec) = self.levels.get_mut(level as usize - 1) {
      vec.push(wrapped);
    }
  }
}
