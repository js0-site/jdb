//! Async file system with Direct I/O
//! 支持 Direct I/O 的异步文件系统

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
mod file;
mod os;

pub use error::{Error, Result};
pub use file::File;
pub use jdb_alloc::PAGE_SIZE;
