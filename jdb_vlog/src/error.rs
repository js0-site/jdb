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

  #[error("header crc mismatch: expected {expected:08x}, got {got:08x}")]
  HeaderCrc { expected: u32, got: u32 },

  #[error("body crc mismatch: expected {expected:08x}, got {got:08x}")]
  BodyCrc { expected: u32, got: u32 },

  #[error("invalid record")]
  InvalidRecord,

  #[error("file not found: {0}")]
  FileNotFound(u64),

  #[error("blob not found: {0}")]
  BlobNotFound(u64),

  #[error("spawn_blocking join failed")]
  Join,
}
