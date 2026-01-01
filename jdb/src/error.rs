// Error types for jdb
// jdb 错误类型定义

use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("WAL error: {0}")]
  Wal(#[from] jdb_val::Error),

  #[error("Corruption detected: {msg}")]
  Corruption { msg: String },

  #[error("Checkpoint corrupted at {path}")]
  CheckpointCorrupt { path: PathBuf },

  #[error("Key too large: {size} > {max}")]
  KeyTooLarge { size: usize, max: usize },

  #[error("Value too large: {size} > {max}")]
  ValueTooLarge { size: usize, max: usize },

  #[error("Invalid namespace ID: {id}")]
  InvalidNamespace { id: u64 },

  #[error("Database already open")]
  AlreadyOpen,

  #[error("Database not open")]
  NotOpen,

  #[error("Import failed: {msg}")]
  ImportFailed { msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;
