//! # PGM-Index: Ultra-Fast Learned Index
//! PGM 索引：超快的学习型索引
//!
//! Piecewise Geometric Model (PGM) Index for fast lookups in sorted arrays.
//! 分段几何模型（PGM）索引，用于在已排序数组中快速查找。
//!
//! ## Usage / 使用方法
//!
//! ```rust
//! use jdb_pgm_index::PGMIndex;
//!
//! let data: Vec<u64> = (0..1_000_000).collect();
//! let pgm = PGMIndex::new(data, 32);
//! assert_eq!(pgm.get(123_456), Some(123_456));
//! ```

mod pgm;
mod pgm_index;

pub use pgm::types::{Key, PGMStats, Segment};
pub use pgm_index::PGMIndex;
