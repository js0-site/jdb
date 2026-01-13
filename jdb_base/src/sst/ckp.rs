//! Checkpoint interface
//! 检查点接口

use std::{fmt::Debug, future::Future};

use crate::sst::{CkpOp, MetaLi};

/// WAL ID type
/// WAL ID 类型
pub type WalId = u64;

/// WAL Offset type
/// WAL 偏移量类型
pub type WalOffset = u64;

/// Checkpoint Manager Trait
/// 检查点管理器特征
pub trait Ckp: Send + Sync + 'static {
  /// Error type
  /// 错误类型
  type Error: Debug + Send;

  /// Get internal Meta list reference
  /// 获取内部 Meta 列表引用
  fn meta_li(&self) -> MetaLi;

  /// Apply operation atomically (Update memory then write disk)
  /// 原子应用操作（先更新内存，再写入磁盘）
  fn apply(&self, op: CkpOp) -> impl Future<Output = Result<(), Self::Error>> + Send {
    async move {
      self.write(bitcode::encode(&op)).await?;
      self.meta_li().update(&op);
      Ok(())
    }
  }

  /// Write operation to persistent storage (WAL/Manifest)
  /// 将操作写入持久化存储（WAL/Manifest）
  fn write(&self, op: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
