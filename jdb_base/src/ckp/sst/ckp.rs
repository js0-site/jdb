use std::{fmt::Debug, future::Future};

use bitcode::{Decode, Encode};

use super::Meta;
use crate::sst::Level;

#[derive(Debug, Clone, Encode, Decode)]
pub enum Op {
  /// Flush MemTable to SST
  /// 刷写 MemTable 到 SST
  Mem2Sst {
    /// New SST metadata
    /// 新 SST 元数据
    meta: Meta,
  },
  /// Compaction
  /// 压缩
  Compact {
    /// Added SSTs
    /// 新增 SST 列表
    add: Vec<Meta>,
    /// Removed SSTs
    /// 移除 SST 列表
    rm: Vec<(Level, Vec<u64>)>,
  },
}

/// Interface for updating Levels state
/// 更新 Levels 状态的接口
pub trait Levels {
  /// Update state with operation (apply only to memory)
  /// 使用操作更新状态（仅应用到内存）
  fn update(&mut self, op: Op);
}

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
      self.levels_mut().update(op);
      Ok(())
    }
  }

  /// Write operation to persistent storage (WAL/Manifest)
  /// 将操作写入持久化存储（WAL/Manifest）
  fn write(&mut self, op: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
