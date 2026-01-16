//! Error types for jdb_mem
//! jdb_mem 的错误类型

use std::fmt::Debug;

/// Memory table error type
/// 内存表错误类型
#[derive(Debug)]
pub enum Error<SstError: Debug> {
  /// SST write/flush error
  /// SST 写入/刷盘错误
  Sst(SstError),
  /// Background flush task disconnected
  /// 后台刷盘任务断开连接
  Disconnect,
}

impl<SstError: Debug> std::fmt::Display for Error<SstError> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Sst(e) => write!(f, "SST error: {:?}", e),
      Self::Disconnect => write!(f, "Flush task disconnected"),
    }
  }
}

impl<E: Debug> std::error::Error for Error<E> {}

/// Result type alias for memory table operations
/// 内存表操作的结果类型别名
pub type Result<T, E> = std::result::Result<T, Error<E>>;
