use thiserror::Error;

/// Error types for checkpoint operations
/// 检查点操作的错误类型
#[derive(Debug, Error)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),
  #[error("Bitcode error: {0}")]
  Bitcode(#[from] bitcode::Error),
  #[error("Log corrupted at position {0}")]
  Corrupted(u64),
}

/// Result type alias for checkpoint operations
/// 检查点操作的结果类型别名
pub type Result<T> = std::result::Result<T, Error>;
