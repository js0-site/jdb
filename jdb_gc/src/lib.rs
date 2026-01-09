//! jdb_gc - GC compression module
//! GC 压缩模块
//!
//! Provides GC trait and LZ4 compression for WAL garbage collection.
//! 提供 GC trait 和 LZ4 压缩用于 WAL 垃圾回收。

mod gc;
mod log;

pub use gc::{Gc, Lz4Gc, MIN_COMPRESS_SIZE, NoGc};
pub use log::{GC_DIR, GcLog};
