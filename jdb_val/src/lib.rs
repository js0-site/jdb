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

use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable, byteorder::little_endian::U64};

/// WAL pointer for checkpoint / WAL 检查点指针
#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct WalPtr {
  pub id: U64,
  pub offset: U64,
}

impl WalPtr {
  #[inline(always)]
  pub fn new(id: u64, offset: u64) -> Self {
    Self {
      id: U64::new(id),
      offset: U64::new(offset),
    }
  }
}

// Internal modules / 内部模块
pub mod error;
pub(crate) mod gc;
pub mod wal;

// Core types / 核心类型
pub use jdb_ckp::{Ckp, After};
pub use error::{Error, Result};
// GC types / GC 类型
pub use gc::{Gcable, IndexUpdate, PosMap};
// Re-export from jdb_base / 从 jdb_base 重新导出
pub use jdb_base::{Flag, Head, HeadBuilder, Load, Pos};
// Test utilities / 测试工具
pub use jdb_base::{HEAD_CRC, HEAD_TOTAL, INFILE_MAX, KEY_MAX, MAGIC};
pub use wal::{
  Conf, DefaultGc, Gc as GcTrait, NoGc, Val, Wal, consts::HEADER_SIZE, record::Record,
};
