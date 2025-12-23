//! 统一错误 Unified error

use thiserror::Error;

pub type R<T> = Result<T, E>;

#[derive(Error, Debug)]
pub enum E {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("checksum: expect {0:#x}, got {1:#x}")]
  Checksum(u32, u32),

  #[error("page not found: {0}")]
  PageNotFound(u32),

  #[error("wal corrupted at {0}")]
  WalCorrupt(u64),

  #[error("not found")]
  NotFound,

  #[error("duplicate")]
  Duplicate,

  #[error("full")]
  Full,

  #[error("{0}")]
  Other(Box<str>),
}

impl E {
  /// 创建 Other 错误 Create Other error
  #[inline]
  pub fn other(msg: impl Into<Box<str>>) -> Self {
    Self::Other(msg.into())
  }
}
