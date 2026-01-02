use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("file locked / 文件已锁定")]
  Locked,

  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
