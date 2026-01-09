//! Error types for jdb_level
//! jdb_level 错误类型定义

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),

  #[error("SST: {0}")]
  Sst(#[from] jdb_sst::Error),

  #[error("Compaction failed")]
  Compact,
}

pub type Result<T> = std::result::Result<T, Error>;
