//! PGM-Index main implementation
//! PGM 索引主实现

use std::mem::size_of;

use crate::{
  Key, PGMStats, Segment,
  pgm::{
    build::{build_lut, build_segments},
    consts::MIN_EPSILON,
    search::{find_seg, predict},
  },
};

/// PGM-Index structure
/// PGM 索引结构
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
#[derive(Clone, Debug)]
pub struct PGMIndex<K: Key> {
  pub epsilon: usize,
  pub data: Vec<K>,
  segments: Vec<Segment<K>>,
  lut: Vec<usize>,
  scale: f64,
  min_key: f64,
}

impl<K: Key> PGMIndex<K> {
  /// Create PGM-Index from sorted data
  /// 从已排序数据创建 PGM 索引
  ///
  /// # Panics
  /// Panics if epsilon < 1, data is empty, or data is not sorted
  /// 如果 epsilon < 1、数据为空或数据未排序则 panic
  pub fn new(data: Vec<K>, epsilon: usize) -> Self {
    assert!(epsilon >= MIN_EPSILON, "epsilon must be >= {MIN_EPSILON}");
    assert!(!data.is_empty(), "data must not be empty");
    debug_assert!(is_sorted(&data), "data must be sorted");

    let segments = build_segments(&data, epsilon);
    let (lut, scale, min_key) = build_lut(&data, &segments);

    Self {
      epsilon,
      data,
      segments,
      lut,
      scale,
      min_key,
    }
  }

  /// Get statistics
  /// 获取统计信息
  #[inline]
  pub fn stats(&self) -> PGMStats {
    PGMStats {
      segments: self.segments.len(),
      avg_segment_size: self.data.len() as f64 / self.segments.len().max(1) as f64,
      memory_bytes: self.mem_usage(),
    }
  }

  #[inline]
  pub fn segment_count(&self) -> usize {
    self.segments.len()
  }

  #[inline]
  pub fn avg_segment_size(&self) -> f64 {
    self.data.len() as f64 / self.segments.len().max(1) as f64
  }

  #[inline]
  pub fn mem_usage(&self) -> usize {
    self.data.len() * size_of::<K>()
      + self.segments.len() * size_of::<Segment<K>>()
      + self.lut.len() * size_of::<usize>()
  }

  /// Alias for mem_usage
  /// mem_usage 的别名
  #[inline]
  pub fn memory_usage(&self) -> usize {
    self.mem_usage()
  }

  /// Get position of key (None if absent)
  /// 获取键的位置（不存在则返回 None）
  #[inline]
  pub fn get(&self, key: K) -> Option<usize> {
    if self.segments.is_empty() {
      return None;
    }

    let sidx = find_seg(key, &self.segments, &self.lut, self.scale, self.min_key);
    // SAFETY: sidx is always valid from find_seg
    // 安全：sidx 总是来自 find_seg 的有效值
    let seg = &self.segments[sidx];

    // Check if key is within segment range
    // 检查键是否在段范围内
    if key < seg.min_key || key > seg.max_key {
      return None;
    }

    let key_f64 = key.as_f64();
    let predicted = predict(seg, key_f64);
    let start = predicted.saturating_sub(self.epsilon).max(seg.start_idx);
    let end = (predicted + self.epsilon + 1).min(seg.end_idx);

    // SAFETY: start..end is within data bounds
    // 安全：start..end 在 data 范围内
    match self.data[start..end].binary_search(&key) {
      Ok(pos) => Some(start + pos),
      Err(_) => None,
    }
  }

  /// Batch lookup
  /// 批量查找
  #[inline]
  pub fn get_many(&self, keys: &[K]) -> Vec<Option<usize>> {
    keys.iter().map(|&k| self.get(k)).collect()
  }

  /// Count hits in batch
  /// 批量命中计数
  #[inline]
  pub fn count_hits(&self, keys: &[K]) -> usize {
    keys.iter().filter(|&&k| self.get(k).is_some()).count()
  }
}

#[inline]
fn is_sorted<K: Ord>(data: &[K]) -> bool {
  data.windows(2).all(|w| w[0] <= w[1])
}
