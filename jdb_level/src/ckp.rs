//! Checkpoint interface
//! 检查点接口

use std::{cell::RefCell, fmt::Debug, future::Future};

use jdb_base::ckp::Op;

use crate::Levels;

/// Checkpoint Manager Trait
/// 检查点管理器特征
pub trait Ckp: Send + Sync + 'static {
  /// Error type
  /// 错误类型
  type Error: Debug + Send;

  /// Get internal Levels state
  /// 获取内部 Levels 状态
  fn levels(&self) -> &RefCell<Levels>;

  /// Apply operation atomically (write disk first, then update memory)
  /// 原子应用操作（先写入磁盘，再更新内存）
  fn apply(&mut self, op: Op) -> impl Future<Output = Result<(), Self::Error>> + Send {
    async move {
      self.write(bitcode::encode(&op)).await?;
      self.levels().borrow_mut().update(&op);
      Ok(())
    }
  }

  /// Write operation to persistent storage (WAL/Manifest)
  /// 将操作写入持久化存储（WAL/Manifest）
  fn write(&mut self, op: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
