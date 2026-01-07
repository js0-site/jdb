//! Checkpoint manager
//! 检查点管理器

use std::collections::{HashMap, VecDeque};

use compio::{fs::File, io::AsyncWriteAtExt};
use jdb_base::{WalId, WalOffset};
use jdb_fs::fs::open_read_write;

use crate::{
  disk::{self, ToDiskBytes},
  error::Result,
  rewrite::rewrite,
  row::{Op, SST_ADD_SIZE, SST_RM_SIZE},
};

/// Checkpoint manager
/// 检查点管理器
pub struct Ckp {
  pub(crate) path: std::path::PathBuf,
  pub(crate) file: File,
  pub(crate) file_pos: u64,
  pub(crate) count: usize,
  pub(crate) truncate: usize,
  pub(crate) keep: usize,
  pub(crate) saves: VecDeque<(WalId, WalOffset)>,
  pub(crate) rotates: Vec<WalId>,
  /// SSTable id -> level mapping
  /// SSTable id -> level 映射
  pub(crate) sst: HashMap<u64, u8>,
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
    self.append(disk::SaveWalPtr::new(wal_id, offset)).await?;

    self.saves.push_back((wal_id, offset));
    if self.saves.len() > self.keep {
      self.saves.pop_front();
    }

    if self.count >= self.truncate {
      self.compact().await?;
    }

    Ok(())
  }

  /// Write rotate
  /// 写入轮转
  pub async fn rotate(&mut self, wal_id: WalId) -> Result<()> {
    self.append(disk::Rotate::new(wal_id)).await?;
    self.rotates.push(wal_id);
    Ok(())
  }

  /// Add or update SSTable level
  /// 添加或更新 SSTable 层级
  pub async fn sst_add(&mut self, id: u64, level: u8) -> Result<()> {
    self.append(disk::SstAdd::new(id, level)).await?;
    self.sst.insert(id, level);
    Ok(())
  }

  /// Remove SSTable
  /// 删除 SSTable
  pub async fn sst_rm(&mut self, id: u64) -> Result<()> {
    self.append(disk::SstRm::new(id)).await?;
    self.sst.remove(&id);
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

    // Single write + sync
    // 单次写入 + sync
    self.file.write_all_at(buf, self.file_pos).await.0?;
    self.file.sync_all().await?;
    self.file_pos += buf_size as u64;
    self.count += ops.len();

    // Update in-memory state
    // 更新内存状态
    for op in ops {
      match op {
        Op::SstAdd(id, level) => {
          self.sst.insert(id, level);
        }
        Op::SstRm(id) => {
          self.sst.remove(&id);
        }
      }
    }

    Ok(())
  }

  /// Get SSTable level
  /// 获取 SSTable 层级
  #[inline]
  pub fn sst_level(&self, id: u64) -> Option<u8> {
    self.sst.get(&id).copied()
  }

  /// Get all SSTable ids with levels
  /// 获取所有 SSTable id 和层级
  #[inline]
  pub fn sst_all(&self) -> &HashMap<u64, u8> {
    &self.sst
  }

  /// Get last saved position
  /// 获取最后保存的位置
  #[inline]
  pub fn wal_id_offset(&self) -> Option<(WalId, WalOffset)> {
    self.saves.back().copied()
  }

  pub(crate) async fn append<const N: usize, T: ToDiskBytes<N>>(&mut self, disk: T) -> Result<u64> {
    let arr = disk.to_array();
    self.file.write_all_at(arr, self.file_pos).await.0?;
    self.file.sync_all().await?;
    let pos = self.file_pos;
    self.file_pos += N as u64;
    self.count += 1;
    Ok(pos)
  }

  pub(crate) async fn compact(&mut self) -> Result<()> {
    // Filter rotates by wal_id
    // 按 wal_id 过滤轮转
    let min_wal_id = self.saves.front().map(|(id, _)| *id).unwrap_or(0);
    self.rotates.retain(|id| *id > min_wal_id);

    self.file_pos = rewrite(&self.path, &self.saves, &self.rotates, &self.sst).await?;
    self.file = open_read_write(&self.path).await?;
    self.count = self.saves.len() + self.rotates.len() + self.sst.len();

    Ok(())
  }
}
