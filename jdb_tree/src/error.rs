//! Error types / 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("page: {0}")]
  Page(#[from] jdb_page::Error),

  #[error("alloc: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  #[error("invalid node type: {0}")]
  InvalidNodeType(u8),

  #[error("node overflow")]
  NodeOverflow,

  #[error("empty tree")]
  EmptyTree,
}
