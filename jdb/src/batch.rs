//! WriteBatch for atomic batch operations
//! 批量写入模块，用于原子批量操作
//!
//! Buffers multiple put/delete operations for atomic commit.
//! 缓冲多个 put/delete 操作以实现原子提交。

use jdb_base::Pos;
use jdb_val::Wal;

use crate::{NsId, NsMgr, Result};

/// Batch operation type
/// 批量操作类型
#[derive(Debug, Clone)]
pub enum Op {
  /// Put key-value pair
  /// 写入键值对
  Put { key: Box<[u8]>, val: Box<[u8]> },
  /// Delete key
  /// 删除键
  Del { key: Box<[u8]> },
}

impl Op {
  /// Get the key of this operation
  /// 获取此操作的键
  #[inline]
  pub fn key(&self) -> &[u8] {
    match self {
      Op::Put { key, .. } | Op::Del { key } => key,
    }
  }

  /// Check if this is a put operation
  /// 检查是否为写入操作
  #[inline]
  pub fn is_put(&self) -> bool {
    matches!(self, Op::Put { .. })
  }

  /// Check if this is a delete operation
  /// 检查是否为删除操作
  #[inline]
  pub fn is_del(&self) -> bool {
    matches!(self, Op::Del { .. })
  }
}

/// Write batch for single namespace (no prefix)
/// 单命名空间的写批次（无前缀）
///
/// Buffers operations in memory before atomic commit.
/// 在原子提交前将操作缓冲在内存中。
#[derive(Debug)]
pub struct Batch {
  /// Namespace ID for this batch
  /// 此批次的命名空间 ID
  ns_id: NsId,
  /// Buffered operations
  /// 缓冲的操作
  ops: Vec<Op>,
}

impl Batch {
  /// Create a new batch for the given namespace
  /// 为给定命名空间创建新批次
  #[inline]
  pub fn new(ns_id: NsId) -> Self {
    Self {
      ns_id,
      ops: Vec::new(),
    }
  }

  /// Create a new batch with preallocated capacity
  /// 创建具有预分配容量的新批次
  #[inline]
  pub fn with_capacity(ns_id: NsId, capacity: usize) -> Self {
    Self {
      ns_id,
      ops: Vec::with_capacity(capacity),
    }
  }

  /// Get the namespace ID
  /// 获取命名空间 ID
  #[inline]
  pub fn ns_id(&self) -> NsId {
    self.ns_id
  }

  /// Add a put operation to the batch
  /// 向批次添加写入操作
  #[inline]
  pub fn put(&mut self, key: &[u8], val: &[u8]) {
    self.ops.push(Op::Put {
      key: key.into(),
      val: val.into(),
    });
  }

  /// Add a delete operation to the batch
  /// 向批次添加删除操作
  #[inline]
  pub fn del(&mut self, key: &[u8]) {
    self.ops.push(Op::Del { key: key.into() });
  }

  /// Get the number of operations in the batch
  /// 获取批次中的操作数量
  #[inline]
  pub fn len(&self) -> usize {
    self.ops.len()
  }

  /// Check if the batch is empty
  /// 检查批次是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.ops.is_empty()
  }

  /// Get the buffered operations
  /// 获取缓冲的操作
  #[inline]
  pub fn ops(&self) -> &[Op] {
    &self.ops
  }

  /// Take ownership of the buffered operations
  /// 获取缓冲操作的所有权
  #[inline]
  pub fn into_ops(self) -> Vec<Op> {
    self.ops
  }

  /// Clear all buffered operations
  /// 清除所有缓冲的操作
  #[inline]
  pub fn clear(&mut self) {
    self.ops.clear();
  }

  /// Commit batch atomically
  /// 原子提交批次
  ///
  /// 1. Write all operations to WAL atomically
  /// 2. Update index atomically
  /// 1. 原子写入所有操作到 WAL
  /// 2. 原子更新索引
  pub async fn commit(self, wal: &mut Wal, ns_mgr: &mut NsMgr) -> Result<()> {
    if self.ops.is_empty() {
      return Ok(());
    }

    // Phase 1: Write all operations to WAL, collect Pos results
    // 阶段 1：写入所有操作到 WAL，收集 Pos 结果
    let mut pos_list: Vec<(Box<[u8]>, Pos)> = Vec::with_capacity(self.ops.len());

    for op in &self.ops {
      let pos = match op {
        Op::Put { key, val } => wal.put(key.as_ref(), val.as_ref()).await?,
        Op::Del { key } => wal.del(key.as_ref()).await?,
      };
      pos_list.push((op.key().into(), pos));
    }

    // Phase 2: Update index atomically (all or nothing in memory)
    // 阶段 2：原子更新索引（内存中全部或无）
    let ns_index = ns_mgr.get(self.ns_id).await?;

    for (key, pos) in pos_list {
      if pos.is_tombstone() {
        ns_index.index.del(key);
      } else {
        ns_index.index.put(key, pos);
      }
    }

    ns_index.dirty = true;
    Ok(())
  }
}
