//! Checkpoint state (implements Compact)
//! 检查点状态（实现 Compact）

use std::collections::{HashMap, VecDeque};

use jdb_base::{WalId, WalOffset};
use jdb_fs::Compact;

use crate::disk;

/// Checkpoint state
/// 检查点状态
pub struct CkpState {
  pub saves: VecDeque<(WalId, WalOffset)>,
  pub rotates: Vec<WalId>,
  pub sst: HashMap<u64, u8>,
  pub keep: usize,
}

impl CkpState {
  pub fn new(keep: usize) -> Self {
    Self {
      saves: VecDeque::new(),
      rotates: Vec::new(),
      sst: HashMap::new(),
      keep,
    }
  }

  /// Filter rotates by min save wal_id
  /// 按最小 save wal_id 过滤 rotates
  pub fn filter_rotates(&mut self) {
    let min_wal_id = self.saves.front().map(|(id, _)| *id).unwrap_or(0);
    self.rotates.retain(|id| *id > min_wal_id);
  }
}

impl Compact for CkpState {
  fn compact_len(&self) -> usize {
    self.saves.len() + self.rotates.len() + self.sst.len()
  }

  fn iter(&self) -> impl Iterator<Item = impl zbin::Bin<'_>> {
    // Sort SST by id for deterministic output
    // 按 id 排序 SST 以保证确定性输出
    let mut sst_vec: Vec<_> = self.sst.iter().collect();
    sst_vec.sort_unstable_by_key(|(id, _)| *id);

    self
      .saves
      .iter()
      .map(|&(wal_id, offset)| disk::SaveWalPtr::new(wal_id, offset).to_array().to_vec())
      .chain(
        self
          .rotates
          .iter()
          .map(|&wal_id| disk::Rotate::new(wal_id).to_array().to_vec()),
      )
      .chain(
        sst_vec
          .into_iter()
          .map(|(&id, &level)| disk::SstAdd::new(id, level).to_array().to_vec()),
      )
  }
}
