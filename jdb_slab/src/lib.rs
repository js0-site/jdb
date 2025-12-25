//! JDB Slab Storage Engine / JDB Slab 存储引擎
//!
//! Single-threaded async Direct I/O slab storage with heat tracking and GC.
//! 基于 compio 的单线程异步 Direct I/O Slab 存储引擎。
#![cfg_attr(docsrs, feature(doc_cfg))]

mod blob;
mod copy;
mod engine;
mod error;
mod gc;
mod header;
mod heat;
mod reader;
mod slab;
mod slot;
mod writer;

pub use blob::BlobStore;
pub use copy::{DEFAULT_BUF_SIZE, pipe, pipe_with, stream_copy, stream_copy_with};
pub use engine::{Engine, SlabConfig};
pub use error::{Error, Result};
pub use gc::Migration;
pub use header::{Compress, Header};
pub use heat::HeatTracker;
pub use reader::SlabReader;
pub use slab::{DEFAULT_BUF_THRESHOLD, SlabClass};
pub use slot::{SlotId, blob_id, decode_slab, encode_slab, is_blob, make_blob};
pub use writer::SlabWriter;
