mod levels;
mod meta;
mod op;

use std::{fmt::Debug, future::Future};

pub use levels::Levels;
pub use meta::Meta;
pub use op::Op;

/// Checkpoint Manager Trait
/// 检查点管理器特征
pub trait Ckp: Send + 'static {
  /// Error type
  /// 错误类型
  type Error: Debug + Send;

  /// Internal Levels state type
  /// 内部 Levels 状态类型
  type Levels: Levels;

  /// Get internal Levels state (mutable)
  /// 获取内部 Levels 状态（可变）
  fn levels_mut(&mut self) -> &mut Self::Levels;

  /// Apply operation atomically (write disk first, then update memory)
  /// 原子应用操作（先写入磁盘，再更新内存）
  fn apply(&mut self, op: Op) -> impl Future<Output = Result<(), Self::Error>> + Send {
    async move {
      self.write(bitcode::encode(&op)).await?;
      self.levels_mut().update(&op);
      Ok(())
    }
  }

  /// Write operation to persistent storage (WAL/Manifest)
  /// 将操作写入持久化存储（WAL/Manifest）
  fn write(&mut self, op: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
