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

  #[error("CRC32 verification failed")]
  Crc32Failed,

  #[error("Record too large")]
  RecordTooLarge,

  #[error("Buffer full")]
  Full,

  #[error("Invalid record format")]
  InvalidRecordFormat,

  #[error("WAL file corrupted")]
  Corrupted,
}