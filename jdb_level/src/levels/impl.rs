use jdb_base::sst::Level;
use sorted_vec::SortedVec;

use super::Levels;
use crate::Meta;

impl Levels {
  /// Push metadata to level
  /// 将元数据推入层级
  #[inline]
  pub(crate) fn push(&mut self, m: jdb_base::ckp::sst::Meta) {
    // Score update handled in update trait, do not duplicate here!
    // 分数更新在 update trait 中处理，不要在这里重复！
    let meta = Meta::new(m.meta, self.lru.clone());
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

  /// Remove metadata by IDs (linear scan, small sets expected)
  /// 通过 ID 移除元数据（线性扫描，预期小集合）
  #[inline]
  pub(crate) fn rm(&mut self, level: Level, ids: impl IntoIterator<Item = u64>) {
    let mut ids: Vec<u64> = ids.into_iter().collect();
    if ids.is_empty() {
      return;
    }
    // Optimization: Sort IDs to allow O(log M) lookup instead of O(M)
    // 优化：对 ID 进行排序，允许 O(log M) 查找而不是 O(M)
    ids.sort_unstable();

    // Score update handled in update trait, do not duplicate here!
    // 分数更新在 update trait 中处理，不要在这里重复！

    let f = |m: &Meta| {
      if ids.binary_search(&m.id).is_ok() {
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
  pub(crate) fn push_iter(&mut self, iter: impl IntoIterator<Item = jdb_base::ckp::sst::Meta>) {
    let mut batch: [Vec<Meta>; Level::LEN] = Default::default();

    for m in iter {
      // SAFETY: Level::LEN is 7, m.sst.level is in 0..=6
      // 安全：Level::LEN 为 7，m.sst.level 在 0..=6 范围内
      // Score update handled in update trait, do not duplicate here!
      // 分数更新在 update trait 中处理，不要在这里重复！
      unsafe {
        batch
          .get_unchecked_mut(m.sst.level as usize)
          .push(Meta::new(m.meta, self.lru.clone()));
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
