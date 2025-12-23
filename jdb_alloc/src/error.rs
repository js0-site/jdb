//! Error types for jdb_alloc
//! jdb_alloc 错误类型

use std::alloc::LayoutError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("invalid layout: {0}")]
  InvalidLayout(#[from] LayoutError),

  #[error("alloc failed")]
  AllocFailed,

  #[error("overflow: {0}/{1}")]
  Overflow(usize, usize),

  #[error("{0}")]
  Other(Box<str>),
}

pub type Result<T> = std::result::Result<T, Error>;
