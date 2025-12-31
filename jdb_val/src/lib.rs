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

pub mod block_cache;
pub mod error;
pub mod flag;
pub mod fs;
pub mod gc;
pub mod head;
pub mod pos;
pub mod wal;

pub use block_cache::BLOCK_SIZE;
pub use error::{Error, Result};
pub use flag::Flag;
pub use fs::{
  decode_id, encode_id, id_path, open_read, open_read_write, open_read_write_create,
  open_write_create, write_file,
};
pub use gc::{
  DefaultGc, GcResult, GcState, Gcable, PosMap, PosMapUpdate, find_idle_core, run_gc,
  run_gc_threaded, should_restart, spawn_gc,
};
pub use head::{
  CRC_SIZE, FilePos, HEAD_CRC, HEAD_SIZE, HEAD_TOTAL, Head, HeadBuilder, INFILE_MAX, KEY_MAX, MAGIC,
};
pub use hipstr::HipByt;
pub use pos::{Pos, RecPos};
pub use wal::{
  Cache, CachedData, Conf, DataStream, DefaultConf, Gc as GcTrait, GcConf, HEADER_SIZE, Lru,
  NoCache, WAL_VERSION, Wal, WalConf, WalInner, WalNoCache,
};
pub use zbin::Bin;
