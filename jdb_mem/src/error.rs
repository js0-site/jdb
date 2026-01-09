//! Error types
//! 错误类型

use thiserror::Error;

/// Flush error
/// 刷盘错误
#[derive(Debug, Error)]
pub enum FlushErr<E> {
  /// Flush operation failed
  /// 刷盘操作失败
  #[error("flush: {0:?}")]
  Flush(E),

  /// Channel recv failed
  /// 通道接收失败
  #[error("flush channel closed")]
  Recv,
}
