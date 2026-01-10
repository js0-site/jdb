#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable writer module
//! SSTable 写入模块

mod core;
mod flush;
mod foot;
mod id;
mod pgm;
mod state;

pub use core::{push, write, write_id, write_stream};

pub use flush::Write;
pub use id::new as gen_id;
