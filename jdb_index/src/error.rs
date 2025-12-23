//! 索引错误 Index errors

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("page: {0}")]
  Page(#[from] jdb_page::error::E),

  #[error("filesystem: {0}")]
  Fs(#[from] jdb_fs::error::E),

  #[error("duplicate key")]
  Duplicate,

  #[error("empty tree")]
  EmptyTree,

  #[error("key not found")]
  NotFound,

  #[error("page full")]
  Full,

  #[error("lock contention")]
  LockContention,

  #[error("page corrupted")]
  PageCorrupted,
}
