//! API errors API 错误

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
  #[error("client not connected")]
  NotConnected,

  #[error("runtime error: {0}")]
  Runtime(#[from] jdb_runtime::RuntimeError),

  #[error("invalid key")]
  InvalidKey,

  #[error("invalid value")]
  InvalidValue,
}

pub type Result<T> = std::result::Result<T, ApiError>;
