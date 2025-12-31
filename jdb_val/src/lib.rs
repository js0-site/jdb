//! # jdb_val - WAL Value Storage
//! WAL 值存储
//!
//! High-performance WAL storage for KV separation architecture.
//! 高性能 WAL 存储，用于 KV 分离架构。
//!
//! ## Storage Modes / 存储模式
//!
//! | Mode   | Description                          |
//! |--------|--------------------------------------|
//! | INFILE | Val in WAL file (≤4MB)               |
//! | FILE   | Val in separate file (>4MB)          |

// Internal modules / 内部模块
pub(crate) mod block_cache;
pub mod checkpoint;
pub mod error;
pub(crate) mod flag;
pub(crate) mod fs;
pub(crate) mod gc;
pub(crate) mod head;
pub(crate) mod pos;
pub(crate) mod record;
pub mod wal;

// Core types / 核心类型
pub use checkpoint::Checkpoint;
pub use error::{Error, Result};
pub use flag::Flag;
// GC types / GC 类型
pub use gc::{Gcable, IndexUpdate, PosMap};
// Test utilities / 测试工具
pub use head::{HEAD_CRC, HEAD_TOTAL, Head, HeadBuilder, MAGIC};
// Limits / 限制常量
pub use head::{INFILE_MAX, KEY_MAX};
pub use pos::Pos;
pub use record::Record;
pub use wal::{Conf, DefaultGc, Gc as GcTrait, NoGc, Val, Wal, consts::HEADER_SIZE};
