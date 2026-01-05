//! Search operations for PGM-Index
//! PGM 索引查找操作

#![allow(
  clippy::cast_possible_truncation,
  clippy::cast_possible_wrap,
  clippy::cast_sign_loss
)]

use crate::{Key, Segment, pgm::consts::ZERO_SLOPE_THRESHOLD};

/// Predict index position using segment's linear model
/// 使用段的线性模型预测索引位置
#[inline]
pub fn predict(seg: &Segment<impl Key>, key_f64: f64) -> usize {
  if seg.slope.abs() < ZERO_SLOPE_THRESHOLD {
    seg.start_idx
  } else {
    // OPT: Use Fused Multiply-Add (FMA) for better precision and speed on modern CPUs.
    // Math: slope * key + intercept + 0.5
    // 优化：使用融合乘加 (FMA) 指令。
    let pos = seg.slope.mul_add(key_f64, seg.intercept) + 0.5;

    // Cast to isize first to handle potential negative underflow from float precision
    let pos_i = pos as isize;

    // OPT: Manual clamp is often faster than std::clamp due to fewer branches in assembly
    // 优化：手动 clamp 通常比 std::clamp 少分支
    let lo = seg.start_idx as isize;
    let hi = (seg.end_idx - 1) as isize;

    if pos_i < lo {
      lo as usize
    } else if pos_i > hi {
      hi as usize
    } else {
      pos_i as usize
    }
  }
}
