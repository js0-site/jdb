//! Runtime errors 运行时错误

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
  #[error("vnode {0} not found")]
  VNodeNotFound(u16),

  #[error("worker {0} not found")]
  WorkerNotFound(usize),

  #[error("channel send failed")]
  SendFailed,

  #[error("channel recv failed")]
  RecvFailed,

  #[error("runtime not started")]
  NotStarted,

  #[error("runtime already started")]
  AlreadyStarted,

  #[error("tablet: {0}")]
  Tablet(#[from] jdb_comm::JdbError),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;
