//! API errors API 错误

use thiserror::Error;

/// 结果类型 Result type
pub type Result<T> = std::result::Result<T, Error>;

/// 错误类型 Error type
#[derive(Debug, Error)]
pub enum Error {
  #[error("not connected")]
  NotConnected,

  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("invalid key")]
  InvalidKey,

  #[error("invalid value")]
  InvalidValue,

  #[error("operation failed: {0}")]
  OperationFailed(String),

  #[error("connection error: {0}")]
  ConnectionError(String),
}

/// 结果类型别名 Result type alias  
pub type ApiResult<T> = std::result::Result<T, Error>;


