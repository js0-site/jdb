//! Error types 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("fs: {0}")]
  Fs(#[from] jdb_fs::Error),

  #[error("alloc: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("checksum mismatch: expected {expected:08x}, got {got:08x}")]
  Checksum { expected: u32, got: u32 },

  #[error("invalid page: {0}")]
  InvalidPage(&'static str),

  #[error("page not found: {0}")]
  NotFound(u64),
}
