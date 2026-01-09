//! Error types for jdb_sst
//! jdb_sst 错误类型定义

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),

  #[error("SSTable too small: {size} bytes")]
  SstTooSmall { size: usize },

  #[error("Invalid foot")]
  InvalidFoot,

  #[error("Checksum mismatch: expected {expected}, got {actual}")]
  ChecksumMismatch { expected: u32, actual: u32 },

  #[error("Invalid filter")]
  InvalidFilter,

  #[error("Invalid offsets")]
  InvalidOffsets,

  #[error("Invalid index block")]
  InvalidIndex,

  #[error("Invalid block at offset {offset}")]
  InvalidBlock { offset: u64 },

  #[error("Failed to build filter")]
  FilterBuildFailed,

  #[error("Key too large: {0} bytes (max 65535)")]
  KeyTooLarge(usize),

  #[error("Compaction failed")]
  Compact,
}

pub type Result<T> = std::result::Result<T, Error>;
