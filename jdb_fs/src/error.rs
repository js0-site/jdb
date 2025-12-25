//! Error types 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("alloc: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("alignment: offset {offset}, len {len}, align {align}")]
  Alignment {
    offset: u64,
    len: usize,
    align: usize,
  },

  #[error("short read: expected {expected}, actual {actual}")]
  ShortRead { expected: usize, actual: usize },

  #[error("short write: expected {expected}, actual {actual}")]
  ShortWrite { expected: usize, actual: usize },

  #[error("spawn_blocking join failed")]
  Join,

  #[error("file size overflow: {0}")]
  Overflow(u64),
}
