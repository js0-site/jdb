//! 错误定义 Error definitions

use thiserror::Error;

/// 结果类型 Result type
pub type R<T> = Result<T, E>;

/// 错误类型 Error type
#[derive(Error, Debug)]
pub enum E {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("CRC32 verification failed")]
  Crc32Failed,

  #[error("Invalid page magic number")]
  InvalidPageMagic,

  #[error("Invalid page ID: {0}")]
  InvalidPageId(u32),

  #[error("Page overflow")]
  PageOverflow,

  #[error("Invalid page type: {0}")]
  InvalidPageType(u8),

  #[error("Invalid data format")]
  InvalidDataFormat,
}