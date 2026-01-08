//! Error types for jdb
//! jdb 错误类型定义

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),

  #[error("WAL: {0}")]
  Wal(#[from] wlog::Error),

  #[error("Checkpoint: {0}")]
  Ckp(#[from] jdb_ckp::Error),

  #[error("SSTable: {0}")]
  SSTable(#[from] jdb_sst::Error),

  #[error("Database closed")]
  Closed,
}

pub type Result<T> = std::result::Result<T, Error>;
