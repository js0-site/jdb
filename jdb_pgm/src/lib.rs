//! # PGM-Index: Ultra-Fast Learned Index
//! PGM 索引：超快的学习型索引
//!
//! Piecewise Geometric Model (PGM) Index for fast lookups in sorted arrays.
//! 分段几何模型（PGM）索引，用于在已排序数组中快速查找。
//!
//! ## Usage / 使用方法
//!
//! ```rust
//! use jdb_pgm::PGMIndex;
//!
//! let data: Vec<u64> = (0..1_000_000).collect();
//! let pgm = PGMIndex::load(data, 32, true).unwrap();
//! assert_eq!(pgm.get(123_456), Some(123_456));
//! ```

pub mod error;
mod pgm;
mod pgm_index;

pub use error::{PGMError, Result};
pub use pgm::types::{Key, PGMStats, Segment};
pub use pgm_index::PGMIndex;

/// Convert key bytes to u64 prefix (big-endian, pad with 0).
/// 将键字节转换为 u64 前缀（大端序，不足补0）。
/// Useful for converting strings/bytes to keys compatible with PGM.
#[cfg(feature = "key_to_u64")]
#[inline]
pub fn key_to_u64(key: &[u8]) -> u64 {
  let len = key.len().min(8);
  let mut buf = [0u8; 8];
  // OPT: Copy slice safely. Compiler optimizes to efficient register moves.
  // Safety: len is clamped to 8, buf is 8, key is at least len.
  // 优化：安全复制切片。编译器会优化为高效的寄存器移动。
  unsafe {
    std::ptr::copy_nonoverlapping(key.as_ptr(), buf.as_mut_ptr(), len);
  }
  u64::from_be_bytes(buf)
}
