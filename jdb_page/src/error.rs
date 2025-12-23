//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type R<T> = Result<T, E>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum E {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Allocation error: {0}")]
  Alloc(#[from] jdb_alloc::error::E),

  #[error("Filesystem error: {0}")]
  Fs(#[from] jdb_fs::error::E),

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

impl From<&str> for E {
  fn from(s: &str) -> Self {
    E::BufferPool(s.to_string())
  }
}