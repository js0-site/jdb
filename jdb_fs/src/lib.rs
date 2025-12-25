//! Async file system with Direct I/O
//! 支持 Direct I/O 的异步文件系统

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
mod file;
pub mod fs;
pub mod os;

pub use error::{Error, Result};
pub use file::File;
pub use fs::{exists, ls, mkdir, remove, rename, size, sync_dir};
pub use jdb_alloc::PAGE_SIZE;
