//! Segment building using Optimal Piecewise Linear Approximation (Shrinking Cone)
//! 使用最优分段线性逼近（收缩锥算法）构建段
//!
//! Time Complexity: O(N)

#![allow(clippy::cast_precision_loss)]

use crate::{
  Key, Segment,
  pgm::consts::{LUT_BINS_MULTIPLIER, MAX_LUT_BINS, MIN_LUT_BINS},
};

/// Build segments using the streaming shrinking cone algorithm.
/// 使用流式收缩锥算法构建段 (O(N))
pub fn build_segments<K: Key>(data: &[K], epsilon: usize) -> Vec<Segment<K>> {
  let n = data.len();
  if n == 0 {
    return vec![];
  }

  // OPT: Heuristic allocation. Assuming compression ratio ~ 2*epsilon is conservative.
  // 优化：启发式分配。假设压缩比约为 2*epsilon。
  let estimated_segments = (n / (epsilon * 2).max(1)).max(16);
  let mut segments = Vec::with_capacity(estimated_segments);

  let mut start = 0;
  let eps = epsilon as f64;
  // SAFETY: n is data.len(), loops are bounded by n.
  // 安全性：n 是 data.len()，循环受 n 约束。
  let ptr = data.as_ptr();

  while start < n {
    // SAFETY: start < n checked by loop
    let first_key = unsafe { (*ptr.add(start)).as_f64() };
    let first_idx = start as f64;

    let mut min_slope = f64::NEG_INFINITY;
    let mut max_slope = f64::INFINITY;

    let mut end = start + 1;

    // Use loop with index for better optimization potential than iterator
    while end < n {
      // SAFETY: end < n checked by loop
      let key = unsafe { (*ptr.add(end)).as_f64() };
      let idx = end as f64;
      let dx = key - first_key;

      if dx == 0.0 {
        if (idx - first_idx) > 2.0 * eps {
          break;
        }
        end += 1;
        continue;
      }

      let slope_lo = (idx - first_idx - eps) / dx;
      let slope_hi = (idx - first_idx + eps) / dx;

      let new_min = min_slope.max(slope_lo);
      let new_max = max_slope.min(slope_hi);

      if new_min > new_max {
        break;
      }

      // Update shrinking cone
      min_slope = new_min;
      max_slope = new_max;
      end += 1;
    }

    let slope = if end == start + 1 {
      0.0
    } else {
      (min_slope + max_slope) * 0.5
    };

    // y = mx + c  =>  c = y - mx
    let intercept = first_idx - slope * first_key; // Standard mul is fine here

    segments.push(Segment {
      min_key: data[start],
      max_key: data[end - 1],
      slope,
      intercept,
      start_idx: start,
      end_idx: end,
    });

    start = end;
  }

  // Optional: only shrink if excess capacity is significant to avoid reallocation cost
  // segments.shrink_to_fit();
  segments
}

/// Build lookup table for fast segment search
/// 构建查找表以快速搜索段
pub fn build_lut<K: Key>(data: &[K], segments: &[Segment<K>]) -> (Vec<usize>, f64, f64) {
  if data.is_empty() || segments.is_empty() {
    return (vec![0], 0.0, 0.0);
  }

  let bins = (segments.len() * LUT_BINS_MULTIPLIER).clamp(MIN_LUT_BINS, MAX_LUT_BINS);

  // SAFETY: Checked empty above
  let min_key = unsafe { data.get_unchecked(0) }.as_f64();
  let max_key = unsafe { data.get_unchecked(data.len() - 1) }.as_f64();

  // Prevent division by zero if all keys are same
  // 防止所有键相同时除以零
  let span = (max_key - min_key).max(1.0);
  let scale = bins as f64 / span;

  let mut lut = vec![0usize; bins + 1];
  let mut seg_idx = 0usize;

  // OPT: Use manual indexing to avoid iterator overhead in hot loop
  // 优化：在热循环中使用手动索引以避免迭代器开销
  // Using `mul_add` for precision in bin calculation: min_key + b * (1/scale)
  // But here we iterate b, so `min_key + b / scale` is standard.
  for b in 0..=bins {
    // Prediction key for this bucket
    let key_at_bin = if scale == 0.0 {
      min_key
    } else {
      min_key + (b as f64) / scale
    };

    while seg_idx + 1 < segments.len() {
      // Advance segment if its max_key is smaller than current bin's target
      let seg_max = unsafe { segments.get_unchecked(seg_idx).max_key }.as_f64();
      if seg_max >= key_at_bin {
        break;
      }
      seg_idx += 1;
    }
    unsafe { *lut.get_unchecked_mut(b) = seg_idx };
  }

  (lut, scale, min_key)
}
