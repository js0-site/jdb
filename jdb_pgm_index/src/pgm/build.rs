//! Segment building for PGM-Index
//! PGM 索引段构建

use crate::{
  Key, Segment,
  pgm::consts::{
    DENOM_TOLERANCE, LUT_BINS_MULTIPLIER, MAX_LUT_BINS, MIN_LUT_BINS, ZERO_SLOPE_THRESHOLD,
  },
};

/// Build segments with epsilon error bound
/// 构建具有 epsilon 误差边界的段
pub fn build_segments<K: Key>(data: &[K], epsilon: usize) -> Vec<Segment<K>> {
  let n = data.len();
  if n == 0 {
    return vec![];
  }

  let mut segments = Vec::new();
  let mut start = 0;
  let eps_f64 = epsilon as f64;

  while start < n {
    // Binary search for optimal segment end
    // 二分查找最优段结束位置
    let mut lo = start + 1;
    let mut hi = n;

    while lo < hi {
      let mid = lo + (hi - lo).div_ceil(2);
      let seg = fit_segment(&data[start..mid], start);
      if check_epsilon(&data[start..mid], &seg, eps_f64) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }

    let seg = fit_segment(&data[start..lo], start);
    segments.push(seg);
    start = lo;
  }

  segments
}

/// Check if segment satisfies epsilon bound
/// 检查段是否满足 epsilon 边界
#[inline]
fn check_epsilon<K: Key>(slice: &[K], seg: &Segment<K>, eps: f64) -> bool {
  let zero_slope = seg.slope.abs() < ZERO_SLOPE_THRESHOLD;

  for (i, &k) in slice.iter().enumerate() {
    let predicted = if zero_slope {
      0.0
    } else {
      (k.as_f64() - seg.intercept) / seg.slope
    };
    if (predicted - i as f64).abs() > eps {
      return false;
    }
  }
  true
}

/// Fit linear segment to data slice
/// 为数据切片拟合线性段
#[inline]
fn fit_segment<K: Key>(slice: &[K], global_start: usize) -> Segment<K> {
  let len = slice.len();
  // SAFETY: slice is guaranteed non-empty by caller
  // 安全：调用者保证 slice 非空
  let min_key = slice[0];
  let max_key = slice[len - 1];

  if len == 1 {
    return Segment {
      min_key,
      max_key,
      slope: 0.0,
      intercept: min_key.as_f64(),
      start_idx: global_start,
      end_idx: global_start + 1,
    };
  }

  // Linear regression: x = index (0..len), y = key value
  // 线性回归：x = 索引 (0..len), y = 键值
  let n = len as f64;
  let sum_x = (len - 1) as f64 * n / 2.0;
  let sum_x2 = (len - 1) as f64 * n * (2.0 * n - 1.0) / 6.0;

  let mut sum_y = 0.0;
  let mut sum_xy = 0.0;
  for (i, &k) in slice.iter().enumerate() {
    let y = k.as_f64();
    sum_y += y;
    sum_xy += i as f64 * y;
  }

  let denom = n * sum_x2 - sum_x * sum_x;
  let (slope, intercept) = if denom.abs() < DENOM_TOLERANCE {
    (0.0, sum_y / n)
  } else {
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    (slope, intercept)
  };

  Segment {
    min_key,
    max_key,
    slope,
    intercept,
    start_idx: global_start,
    end_idx: global_start + len,
  }
}

/// Build lookup table for fast segment search
/// 构建查找表以快速搜索段
pub fn build_lut<K: Key>(data: &[K], segments: &[Segment<K>]) -> (Vec<usize>, f64, f64) {
  let bins = (segments.len() * LUT_BINS_MULTIPLIER).clamp(MIN_LUT_BINS, MAX_LUT_BINS);
  // SAFETY: data is guaranteed non-empty by caller
  // 安全：调用者保证 data 非空
  let min_key = data[0].as_f64();
  let max_key = data[data.len() - 1].as_f64();
  let span = (max_key - min_key).max(1.0);
  let scale = bins as f64 / span;

  let mut lut = vec![0usize; bins + 1];
  let mut seg_idx = 0usize;

  for (b, entry) in lut.iter_mut().enumerate() {
    let key_at_bin = min_key + b as f64 / scale;
    while seg_idx + 1 < segments.len() && segments[seg_idx].max_key.as_f64() < key_at_bin {
      seg_idx += 1;
    }
    *entry = seg_idx;
  }

  (lut, scale, min_key)
}
