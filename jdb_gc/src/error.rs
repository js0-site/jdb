//! GC errors / GC 错误

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),

  #[error("jdb_page: {0}")]
  Page(#[from] jdb_page::Error),

  #[error("jdb_vlog: {0}")]
  Vlog(#[from] jdb_vlog::Error),
}
