use std::collections::BTreeSet;

use bitcode::{Decode, Encode};

pub use super::Meta;

/// Checkpoint Operation
/// 检查点操作
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
    adds: BTreeSet<Meta>,
    /// Removed SST IDs
    /// 移除 SST ID 列表
    rms: Vec<u64>,
  },
}
