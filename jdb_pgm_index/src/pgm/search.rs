//! Search operations for PGM-Index
//! PGM 索引查找操作

use crate::{Key, Segment, pgm::consts::ZERO_SLOPE_THRESHOLD};

/// Predict index position using segment's linear model
/// 使用段的线性模型预测索引位置
#[inline]
pub fn predict(seg: &Segment<impl Key>, key_f64: f64) -> usize {
  if seg.slope.abs() < ZERO_SLOPE_THRESHOLD {
    seg.start_idx
  } else {
    let local_x = (key_f64 - seg.intercept) / seg.slope;
    let global_x = local_x + seg.start_idx as f64;
    // SAFETY: clamp ensures valid range
    // 安全：clamp 确保有效范围
    (global_x.round() as isize).clamp(seg.start_idx as isize, (seg.end_idx - 1) as isize) as usize
  }
}

/// Find segment containing the key using lookup table
/// 使用查找表查找包含键的段
#[inline]
pub fn find_seg<K: Key>(
  key: K,
  segments: &[Segment<K>],
  lut: &[usize],
  scale: f64,
  min_key: f64,
) -> usize {
  if segments.len() <= 1 {
    return 0;
  }

  let y = key.as_f64();
  let bin = ((y - min_key) * scale)
    .floor()
    .clamp(0.0, (lut.len() - 1) as f64) as usize;
  let mut idx = lut[bin];

  // Adjust forward
  // 向前调整
  while idx + 1 < segments.len() && key > segments[idx].max_key {
    idx += 1;
  }
  // Adjust backward
  // 向后调整
  while idx > 0 && key < segments[idx].min_key {
    idx -= 1;
  }
  idx
}
