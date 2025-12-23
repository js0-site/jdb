//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type R<T> = Result<T, E>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum E {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Metadata not found")]
  NotFound,

  #[error("Invalid metadata format")]
  InvalidFormat,

  #[error("Metadata corrupted")]
  Corrupted,

  #[error("Version mismatch")]
  VersionMismatch,
}