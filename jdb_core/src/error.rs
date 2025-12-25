//! Error types / 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("db: {0}")]
  Db(#[from] jdb_db::Error),

  #[error("tree: {0}")]
  Tree(#[from] jdb_tree::Error),

  #[error("vlog: {0}")]
  VLog(#[from] jdb_vlog::Error),

  #[error("page: {0}")]
  Page(#[from] jdb_page::Error),

  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("db not found: {0}")]
  DbNotFound(u64),
}
