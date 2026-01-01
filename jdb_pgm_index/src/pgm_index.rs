//! PGM-Index main implementation
//! PGM 索引主实现

use crate::{
  Key, PGMStats, Segment,
  pgm::{
    build::{build_lookup_table, build_segments},
    consts::MIN_EPSILON,
    search::{find_segment, predict_index},
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
  segment_lookup: Vec<usize>,
  lookup_scale: f64,
  min_key_f64: f64,
}

impl<K: Key> PGMIndex<K> {
  /// Create PGM-Index from sorted data
  /// 从已排序数据创建 PGM 索引
  pub fn new(data: Vec<K>, epsilon: usize) -> Self {
    assert!(epsilon >= MIN_EPSILON, "epsilon must be >= {MIN_EPSILON}");
    assert!(!data.is_empty(), "data must not be empty");
    assert!(is_sorted(&data), "data must be sorted");

    let segments = build_segments(&data, epsilon);
    let (segment_lookup, lookup_scale, min_key_f64) = build_lookup_table(&data, &segments);

    Self {
      epsilon,
      data,
      segments,
      segment_lookup,
      lookup_scale,
      min_key_f64,
    }
  }

  /// Get statistics
  /// 获取统计信息
  pub fn stats(&self) -> PGMStats {
    PGMStats {
      segments: self.segment_count(),
      avg_segment_size: self.avg_segment_size(),
      memory_bytes: self.memory_usage(),
    }
  }

  pub fn segment_count(&self) -> usize {
    self.segments.len()
  }

  pub fn avg_segment_size(&self) -> f64 {
    (self.data.len() as f64) / (self.segments.len() as f64).max(1.0)
  }

  pub fn memory_usage(&self) -> usize {
    let data_bytes = self.data.len() * std::mem::size_of::<K>();
    let seg_bytes = self.segments.len() * std::mem::size_of::<Segment<K>>();
    let lut_bytes = self.segment_lookup.len() * std::mem::size_of::<usize>();
    data_bytes + seg_bytes + lut_bytes
  }

  /// Get position of key (None if absent)
  /// 获取键的位置（不存在则返回 None）
  pub fn get(&self, key: K) -> Option<usize> {
    if self.segments.is_empty() {
      return None;
    }

    let sidx = find_segment(
      key,
      &self.segments,
      &self.segment_lookup,
      self.lookup_scale,
      self.min_key_f64,
    );
    let seg = &self.segments[sidx];

    // Check if key is within segment range
    // 检查键是否在段范围内
    if key < seg.min_key || key > seg.max_key {
      return None;
    }

    let predicted = predict_index(seg, key);
    let eps = self.epsilon;
    let start = predicted.saturating_sub(eps).max(seg.start_idx);
    let end = (predicted + eps + 1).min(seg.end_idx);

    let slice = &self.data[start..end];
    match slice.binary_search(&key) {
      Ok(pos) => Some(start + pos),
      Err(_) => None,
    }
  }

  /// Batch lookup
  /// 批量查找
  pub fn get_many(&self, keys: &[K]) -> Vec<Option<usize>> {
    keys.iter().map(|&k| self.get(k)).collect()
  }

  /// Count hits in batch
  /// 批量命中计数
  pub fn count_hits(&self, keys: &[K]) -> usize {
    keys.iter().filter(|&&k| self.get(k).is_some()).count()
  }
}

fn is_sorted<K: Ord>(data: &[K]) -> bool {
  data.windows(2).all(|w| w[0] <= w[1])
}
