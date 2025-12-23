//! API errors API 错误

use thiserror::Error;

/// 结果类型 Result type
pub type R<T> = Result<T, E>;

/// 错误类型 Error type
#[derive(Debug, Error)]
pub enum E {
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


