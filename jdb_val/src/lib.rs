//! # jdb_val - WAL Value Storage / WAL 值存储
//!
//! Key and Value have 3 storage modes / Key 和 Value 有 3 种存储方式:
//!
//! | Mode   | Flag        | Description                          |
//! |--------|-------------|--------------------------------------|
//! | INLINE | 0x00        | Embedded in Head / 内联在 Head 中     |
//! | INFILE | 0x01-0x03   | Same WAL file / 同一 WAL 文件内       |
//! | FILE   | 0x04-0x06   | Separate file / 独立文件              |
//!
//! ## Layout Combinations / 布局组合
//!
//! | Key      | Val      | Use Case                              |
//! |----------|----------|---------------------------------------|
//! | INLINE   | INLINE   | Small KV (key+val <= 52B)             |
//! | INLINE   | INFILE   | Small key, medium val                 |
//! | INLINE   | FILE     | Small key, large val                  |
//! | INFILE   | INLINE   | Medium key, small val                 |
//! | INFILE   | INFILE   | Medium KV                             |
//! | INFILE   | FILE     | Medium key, large val                 |
//! | FILE     | INLINE   | Large key, small val                  |
//! | FILE     | INFILE   | Large key, medium val                 |
//! | FILE     | FILE     | Large KV                              |
//!
//! ## Compression / 压缩
//!
//! INFILE and FILE modes support compression / INFILE 和 FILE 模式支持压缩:
//! - `*_LZ4`: LZ4 compression / LZ4 压缩
//! - `*_ZSTD`: ZSTD compression / ZSTD 压缩

pub mod error;
pub mod flag;
pub mod gc;
pub mod gen_id;
pub mod head;
pub mod loc;
pub mod wal;

pub use error::{Error, Result};
pub use flag::Flag;
pub use gc::{Gc, GcState};
pub use gen_id::GenId;
pub use head::{Head, INFILE_MAX};
pub use loc::Loc;
pub use wal::{Conf, Wal, HEADER_SIZE, WAL_VERSION};
