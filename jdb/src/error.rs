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

  #[error("Checkpoint error: {0}")]
  Ckp(#[from] jdb_ckp::Error),

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

  #[error("SSTable too small: {size} bytes")]
  SstTooSmall { size: u64 },

  #[error("Invalid footer")]
  InvalidFooter,

  #[error("Checksum mismatch: expected {expected}, got {actual}")]
  ChecksumMismatch { expected: u32, actual: u32 },

  #[error("Invalid filter")]
  InvalidFilter,

  #[error("Invalid offsets")]
  InvalidOffsets,

  #[error("Invalid block at offset {offset}")]
  InvalidBlock { offset: u64 },

  #[error("Failed to build filter")]
  FilterBuildFailed,
}

pub type Result<T> = std::result::Result<T, Error>;
