//! jdb_gc - GC compression module
//! GC 压缩模块
//!
//! Provides GC trait and LZ4 compression for WAL garbage collection.
//! 提供 GC trait 和 LZ4 压缩用于 WAL 垃圾回收。

mod gc;

pub use gc::{Gc, Lz4Gc, NoGc};
pub use jdb_base::{Flag, Pos};
pub use jdb_fs::head::Head;

/// Min data size for compression (1KB)
/// 压缩最小数据大小（1KB）
pub const MIN_COMPRESS_SIZE: usize = 1024;
