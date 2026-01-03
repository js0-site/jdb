//! Error types for checkpoint operations
//! 检查点操作的错误类型

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
