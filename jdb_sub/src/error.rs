//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type Result<T> = std::result::Result<T, Error>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Subscription not found")]
  NotFound,

  #[error("Invalid subscription format")]
  InvalidFormat,

  #[error("Subscription already exists")]
  AlreadyExists,

  #[error("Subscription error: {0}")]
  SubscriptionError(String),
}

/// 结果类型别名 Result type alias  
pub type SubResult<T> = std::result::Result<T, Error>;