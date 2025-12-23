//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type Result<T> = std::result::Result<T, Error>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum Error {
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

/// 结果类型别名 Result type alias  
pub type MetaResult<T> = std::result::Result<T, Error>;