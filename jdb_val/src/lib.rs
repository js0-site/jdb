//! # jdb_val - WAL Value Storage
//! WAL 值存储
//!
//! High-performance WAL storage for KV separation architecture.
//! 高性能 WAL 存储，用于 KV 分离架构。
//!
//! ## Storage Modes
//! 存储模式
//!
//! | Mode   | Description                          |
//! |--------|--------------------------------------|
//! | INFILE | Val in WAL file (≤4MB)               |
//! | FILE   | Val in separate file (>4MB)          |

// Internal modules
// 内部模块
pub(crate) mod block_cache;
pub mod error;
pub(crate) mod flag;
pub(crate) mod fs;
pub(crate) mod gc;
pub(crate) mod head;
pub(crate) mod pos;
pub mod wal;

// Core types / 核心类型
pub use error::{Error, Result};
pub use flag::Flag;
// GC types / GC 类型
pub use gc::{DefaultGc, GcState, Gcable, IndexUpdate, PosMap};
// Test exports (for tests/ directory)
// 测试导出（用于 tests/ 目录）
#[doc(hidden)]
pub use head::{HEAD_CRC, HEAD_TOTAL, Head, HeadBuilder, MAGIC};
// Limits / 限制
pub use head::{INFILE_MAX, KEY_MAX};
pub use pos::Pos;
#[doc(hidden)]
pub use pos::RecPos;
#[doc(hidden)]
pub use wal::consts::HEADER_SIZE;
pub use wal::{Conf, Gc as GcTrait, Val, Wal};
