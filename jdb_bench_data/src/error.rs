// Error types for jdb_bench_data
// jdb_bench_data 错误类型

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
