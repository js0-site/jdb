//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type R<T> = Result<T, E>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum E {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Allocation error: {0}")]
  Alloc(#[from] jdb_alloc::error::E),

  #[error("Filesystem error: {0}")]
  Fs(#[from] jdb_fs::error::E),

  #[error("Compression error: {0}")]
  Compression(#[from] jdb_compress::error::E),

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