//! Checkpoint manager
//! 检查点管理器

use std::collections::VecDeque;

use compio::{fs::File, io::AsyncWriteAtExt};
pub use jdb_base::{WalId, WalOffset};
use jdb_fs::open_read_write;

use crate::{error::Result, rewrite::rewrite, row::Row};

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
  pub async fn set(&mut self, wal_id: WalId, offset: WalOffset) -> Result<()> {
    self.append(Row::Save { wal_id, offset }).await?;

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
    self.append(Row::Rotate { wal_id }).await?;
    self.rotates.push(wal_id);
    Ok(())
  }

  /// Get last saved position
  /// 获取最后保存的位置
  #[inline]
  pub fn wal_id_offset(&self) -> Option<(WalId, WalOffset)> {
    self.saves.back().copied()
  }

  pub(crate) async fn append(&mut self, row: Row) -> Result<u64> {
    let data = row.to_vec();
    let pos = self.file_pos;
    let len = data.len() as u64;

    self.file.write_all_at(data, pos).await.0?;
    self.file.sync_all().await?;

    self.file_pos += len;
    self.count += 1;

    Ok(pos)
  }

  pub(crate) async fn compact(&mut self) -> Result<()> {
    // Filter rotates by wal_id
    // 按 wal_id 过滤轮转
    let min_wal_id = self.saves.front().map(|(id, _)| *id).unwrap_or(0);
    self.rotates.retain(|id| *id > min_wal_id);

    self.file_pos = rewrite(&self.path, &self.saves, &self.rotates).await?;
    self.file = open_read_write(&self.path).await?;
    self.count = self.saves.len() + self.rotates.len();

    Ok(())
  }
}
