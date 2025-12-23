//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type Result<T> = std::result::Result<T, Error>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Compression error: {0}")]
  Compression(String),

  #[error("Invalid blob pointer")]
  InvalidBlobPtr,

  #[error("Checksum mismatch: expected {expected}, actual {actual}")]
  Checksum { expected: u32, actual: u32 },

  #[error("Data not found")]
  NotFound,
}

impl From<jdb_compress::Error> for Error {
  fn from(err: jdb_compress::Error) -> Self {
    Self::Compression(err.to_string())
  }
}