//! # jdb_val - WAL Value Storage
//! WAL 值存储
//!
//! Key and Value have 2 storage modes:
//! Key 和 Value 有 2 种存储方式:
//!
//! | Mode   | Description                          |
//! |--------|--------------------------------------|
//! | INFILE | Same WAL file / 同一 WAL 文件内       |
//! | FILE   | Separate file / 独立文件              |
//!
//! ## Compression
//! 压缩
//!
//! Both modes support compression:
//! 两种模式都支持压缩:
//! - `*_LZ4`: LZ4 compression / LZ4 压缩
//! - `*_ZSTD`: ZSTD compression / ZSTD 压缩
//! - `*_PROBED`: Incompressible / 不可压缩

pub mod error;
pub mod flag;
pub mod gc;
pub mod head;
pub mod pos;
pub mod wal;

pub use error::{Error, Result};
pub use flag::{Flag, Store};
pub use gc::{
  DefaultGc, GcResult, GcState, Gcable, PosMap, PosMapUpdate, find_idle_core, run_gc,
  run_gc_threaded, should_restart, spawn_gc,
};
pub use head::{
  CRC_SIZE, FILE_ENTRY_SIZE, FIXED_SIZE, FilePos, Head, HeadBuilder, INFILE_MAX, MAGIC, MAX_HKV_LEN,
};
pub use hipstr::HipByt;
pub use pos::Pos;
pub use wal::{
  Cache, CachedData, Conf, DataStream, DefaultConf, Gc as GcTrait, GcConf, HEADER_SIZE, Lru,
  NoCache, WAL_VERSION, Wal, WalConf, WalInner, WalNoCache,
};
pub use zbin::Bin;
