//! API errors API 错误

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
  #[error("not connected")]
  NotConnected,

  #[error("tablet error: {0}")]
  Tablet(#[from] jdb_comm::JdbError),

  #[error("invalid key")]
  InvalidKey,

  #[error("invalid value")]
  InvalidValue,
}

pub type Result<T> = std::result::Result<T, ApiError>;
