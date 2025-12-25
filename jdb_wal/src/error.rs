//! WAL errors / WAL 错误

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),

  #[error("jdb_fs: {0}")]
  Fs(#[from] jdb_fs::Error),

  #[error("jdb_alloc: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("CRC mismatch: expected {expected}, got {got}")]
  Crc { expected: u32, got: u32 },

  #[error("Invalid record")]
  InvalidRecord,

  #[error("Incomplete record")]
  Incomplete,
}
