//! Checkpoint manager
//! 检查点管理器

use std::collections::HashMap;

use jdb_base::{WalId, WalOffset};
use jdb_fs::AutoCompact;

use crate::{
  disk::{self, SST_ADD_SIZE, SST_RM_SIZE},
  error::Result,
  row::Op,
  state::CkpState,
};

/// Checkpoint manager
/// 检查点管理器
pub struct Ckp {
  pub(crate) log: AutoCompact<CkpState>,
}

/// Configuration
/// 配置
#[derive(Clone, Copy, Debug)]
pub enum Conf {
  /// Compaction threshold
  /// 压缩阈值
  Truncate(usize),
  /// Keep N saves
  /// 保留 N 个保存点
  Keep(usize),
}

impl Ckp {
  /// Write checkpoint
  /// 写入检查点
  pub async fn set_wal_ptr(&mut self, wal_id: WalId, offset: WalOffset) -> Result<()> {
    self
      .log
      .append(disk::SaveWalPtr::new(wal_id, offset).to_array())
      .await?;

    self.log.inner.saves.push_back((wal_id, offset));
    if self.log.inner.saves.len() > self.log.inner.keep {
      self.log.inner.saves.pop_front();
    }

    self.log.maybe_compact().await?;
    Ok(())
  }

  /// Write rotate
  /// 写入轮转
  pub async fn rotate(&mut self, wal_id: WalId) -> Result<()> {
    self
      .log
      .append(disk::Rotate::new(wal_id).to_array())
      .await?;
    self.log.inner.rotates.push(wal_id);
    Ok(())
  }

  /// Add or update SSTable level
  /// 添加或更新 SSTable 层级
  pub async fn sst_add(&mut self, id: u64, level: u8) -> Result<()> {
    self
      .log
      .append(disk::SstAdd::new(id, level).to_array())
      .await?;
    self.log.inner.sst.insert(id, level);
    Ok(())
  }

  /// Remove SSTable
  /// 删除 SSTable
  pub async fn sst_rm(&mut self, id: u64) -> Result<()> {
    self.log.append(disk::SstRm::new(id).to_array()).await?;
    self.log.inner.sst.remove(&id);
    Ok(())
  }

  /// Atomic batch operations (single sync)
  /// 原子批量操作（单次 sync）
  pub async fn batch(&mut self, ops: impl IntoIterator<Item = Op>) -> Result<()> {
    let ops: Vec<Op> = ops.into_iter().collect();
    if ops.is_empty() {
      return Ok(());
    }

    // Calculate buffer size
    // 计算缓冲区大小
    let buf_size: usize = ops
      .iter()
      .map(|op| match op {
        Op::SstAdd(..) => SST_ADD_SIZE,
        Op::SstRm(_) => SST_RM_SIZE,
      })
      .sum();

    let mut buf = Vec::with_capacity(buf_size);

    // Serialize all ops
    // 序列化所有操作
    for op in &ops {
      match op {
        Op::SstAdd(id, level) => {
          buf.extend_from_slice(&disk::SstAdd::new(*id, *level).to_array());
        }
        Op::SstRm(id) => {
          buf.extend_from_slice(&disk::SstRm::new(*id).to_array());
        }
      }
    }

    let n = ops.len();
    self.log.append_n(buf, n).await?;

    // Update in-memory state
    // 更新内存状态
    for op in ops {
      match op {
        Op::SstAdd(id, level) => {
          self.log.inner.sst.insert(id, level);
        }
        Op::SstRm(id) => {
          self.log.inner.sst.remove(&id);
        }
      }
    }

    Ok(())
  }

  /// Get SSTable level
  /// 获取 SSTable 层级
  #[inline]
  pub fn sst_level(&self, id: u64) -> Option<u8> {
    self.log.inner.sst.get(&id).copied()
  }

  /// Get all SSTable ids with levels
  /// 获取所有 SSTable id 和层级
  #[inline]
  pub fn sst_all(&self) -> &HashMap<u64, u8> {
    &self.log.inner.sst
  }

  /// Get last saved position
  /// 获取最后保存的位置
  #[inline]
  pub fn wal_id_offset(&self) -> Option<(WalId, WalOffset)> {
    self.log.inner.saves.back().copied()
  }

  /// Force compact
  /// 强制压缩
  pub async fn compact(&mut self) -> Result<()> {
    self.log.inner.filter_rotates();
    self.log.compact().await?;
    Ok(())
  }
}
