//! 索引错误 Index errors

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("page: {0}")]
  Page(#[from] jdb_comm::E),

  #[error("duplicate key")]
  Duplicate,

  #[error("empty tree")]
  EmptyTree,

  #[error("key not found")]
  NotFound,

  #[error("page full")]
  Full,
}
