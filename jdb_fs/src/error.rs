//! Error types for jdb_fs
//! jdb_fs 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("alloc: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("invalid offset: {0}")]
  InvalidOffset(u64),

  #[error("invalid page number: {0}")]
  InvalidPageNumber(u32),

  #[error("page not aligned")]
  PageNotAligned,

  #[error("{0}")]
  Other(Box<str>),
}

impl Error {
  /// 创建 Other 错误 Create Other error
  #[inline]
  pub fn other(msg: impl Into<Box<str>>) -> Self {
    Self::Other(msg.into())
  }
}