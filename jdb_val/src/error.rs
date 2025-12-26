use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("key length {0} exceeds limit {1}")]
  KeyTooLong(usize, usize),

  #[error("value length {0} exceeds limit {1}")]
  ValTooLong(usize, usize),

  #[error("invalid flag combination: key={0:?} val={1:?}")]
  InvalidFlag(u8, u8),

  #[error("WAL not open / WAL 未打开")]
  NotOpen,

  #[error("file not found: {0}")]
  FileNotFound(u64),

  #[error("invalid head / 无效的头")]
  InvalidHead,

  #[error("cannot remove current WAL / 不能删除当前 WAL")]
  CannotRemoveCurrent,

  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
