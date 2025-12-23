//! Error types for jdb_alloc
//! jdb_alloc 错误类型

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("invalid layout: {0}")]
  InvalidLayout(String),

  #[error("allocation failed")]
  AllocFailed,

  #[error("buffer overflow: requested {requested}, capacity {capacity}")]
  BufferOverflow { requested: usize, capacity: usize },

  #[error("{0}")]
  Other(Box<str>),
}

pub type Result<T> = std::result::Result<T, Error>;
