//! Type definitions for PGM-Index
//! PGM 索引类型定义

#![allow(clippy::cast_precision_loss, clippy::cast_lossless)]

use std::fmt::Debug;

/// Key trait for supported types
/// 支持的键类型约束
pub trait Key: Copy + Send + Sync + Ord + Debug + 'static {
  /// Convert to f64 (always succeeds for integer types)
  /// 转换为 f64（整数类型总是成功）
  fn as_f64(self) -> f64;
}

// OPT: Inline heavily used trait methods
macro_rules! impl_key {
  ($($t:ty),*) => {
    $(
      impl Key for $t {
        #[inline(always)]
        fn as_f64(self) -> f64 {
          self as f64
        }
      }
    )*
  };
}

impl_key!(
  u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, usize, isize
);

/// Linear segment: y = slope * x + intercept
/// 线性段：y = slope * x + intercept
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
#[derive(Clone, Copy, Debug)]
#[repr(C, align(64))]
pub struct Segment<K: Key> {
  pub min_key: K,
  pub max_key: K,
  pub slope: f64,
  pub intercept: f64,
  pub start_idx: usize,
  pub end_idx: usize,
}

/// Index statistics
/// 索引统计信息
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
#[derive(Clone, Debug, Default)]
pub struct PGMStats {
  pub segments: usize,
  pub avg_segment_size: f64,
  pub memory_bytes: usize,
}
