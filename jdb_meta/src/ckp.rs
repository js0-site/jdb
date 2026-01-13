//! Checkpoint interface
//! 检查点接口

use std::{fmt::Debug, future::Future};

use jdb_base::ckp::Op;

use crate::MetaLi;

/// Checkpoint Manager Trait
/// 检查点管理器特征
pub trait Ckp: Send + Sync + 'static {
  /// Error type
  /// 错误类型
  type Error: Debug + Send;

  /// Execute closure with MetaLi reference
  /// 使用 MetaLi 引用执行闭包
  fn with_meta_li<R>(&self, callback: impl FnOnce(&MetaLi) -> R) -> R;

  /// Update internal MetaLi state
  /// 更新内部 MetaLi 状态
  fn update_meta(&self, op: &Op);

  /// Apply operation atomically (Update memory then write disk)
  /// 原子应用操作（先更新内存，再写入磁盘）
  fn apply(&self, op: Op) -> impl Future<Output = Result<(), Self::Error>> + Send {
    async move {
      self.write(bitcode::encode(&op)).await?;
      self.update_meta(&op);
      Ok(())
    }
  }

  /// Write operation to persistent storage (WAL/Manifest)
  /// 将操作写入持久化存储（WAL/Manifest）
  fn write(&self, op: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
