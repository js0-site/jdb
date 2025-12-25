//! Error types / 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("table: {0}")]
  Table(#[from] jdb_table::Error),

  #[error("tree: {0}")]
  Tree(#[from] jdb_tree::Error),

  #[error("vlog: {0}")]
  VLog(#[from] jdb_vlog::Error),

  #[error("page: {0}")]
  Page(#[from] jdb_page::Error),

  #[error("gc: {0}")]
  Gc(#[from] jdb_gc::Error),

  #[error("fs: {0}")]
  Fs(#[from] jdb_fs::Error),

  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("db not found: {0}")]
  DbNotFound(u64),
}
