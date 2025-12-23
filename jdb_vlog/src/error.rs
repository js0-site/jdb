//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type Result<T> = std::result::Result<T, Error>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Allocation error: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("Filesystem error: {0}")]
  Fs(#[from] jdb_fs::Error),

  #[error("Compression error: {0}")]
  Compression(#[from] jdb_compress::Error),

  #[error("CRC32 verification failed")]
  Crc32Failed,

  #[error("Invalid blob pointer")]
  InvalidBlobPtr,

  #[error("Data not found")]
  NotFound,

  #[error("Checksum mismatch: expected {0}, got {1}")]
  Checksum(u32, u32),

  #[error("Record too large")]
  RecordTooLarge,

  #[error("VLog file corrupted")]
  Corrupted,
}