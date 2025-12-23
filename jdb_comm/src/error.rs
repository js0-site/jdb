//! Unified error handling 统一错误处理

use crate::PageID;
use hipstr::HipStr;
use thiserror::Error;

pub type JdbResult<T> = Result<T, JdbError>;

#[derive(Error, Debug)]
pub enum JdbError {
  #[error("IO: {0}")]
  Io(#[from] std::io::Error),

  #[error("serialize: {0}")]
  Serialize(HipStr<'static>),

  #[error("checksum mismatch: expected {expected:#x}, got {actual:#x}")]
  Checksum { expected: u32, actual: u32 },

  #[error("page not found: {0:?}")]
  PageNotFound(PageID),

  #[error("page size mismatch: expected {expected}, got {actual}")]
  PageSizeMismatch { expected: usize, actual: usize },

  #[error("no tablet available")]
  NoTablet,

  #[error("WAL full")]
  WalFull,

  #[error("internal: {0}")]
  Internal(HipStr<'static>),
}
