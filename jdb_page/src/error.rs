//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type Result<T> = std::result::Result<T, Error>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Allocation error: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("Filesystem error: {0}")]
  Fs(#[from] jdb_fs::Error),

  #[error("All frames pinned")]
  AllFramesPinned,

  #[error("Invalid page ID: {0}")]
  InvalidPageId(u32),

  #[error("Page not found: {0}")]
  PageNotFound(u32),

  #[error("Pool capacity exceeded")]
  CapacityExceeded,

  #[error("Buffer pool error: {0}")]
  BufferPool(String),
}

impl From<&str> for Error {
  fn from(s: &str) -> Self {
    Error::BufferPool(s.to_string())
  }
}