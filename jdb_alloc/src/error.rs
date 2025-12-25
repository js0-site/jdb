//! Error types for jdb_alloc
//! jdb_alloc 错误类型

use std::alloc::LayoutError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("invalid layout: {0}")]
  InvalidLayout(#[from] LayoutError),

  #[error("alloc failed")]
  AllocFailed,

  #[error("overflow: {0}/{1}")]
  Overflow(usize, usize),
}

pub type Result<T> = std::result::Result<T, Error>;
