//! JDB Slab Storage Engine / JDB Slab 存储引擎
//!
//! Single-threaded async Direct I/O slab storage with heat tracking and GC.
//! 基于 compio 的单线程异步 Direct I/O Slab 存储引擎。
#![cfg_attr(docsrs, feature(doc_cfg))]

mod copy;
mod engine;
mod error;
mod gc;
mod header;
mod heat;
mod reader;
mod slab;
mod writer;

pub use copy::{DEFAULT_BUF_SIZE, pipe, pipe_with, stream_copy, stream_copy_with};
pub use engine::{Engine, SlabConfig};
pub use error::{Error, Result};
pub use gc::{GcWorker, Migration};
pub use header::{Compress, Header};
pub use heat::{HeatTracker, SlotId};
pub use reader::SlabReader;
pub use slab::SlabClass;
pub use writer::SlabWriter;
