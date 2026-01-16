mod iter;
mod r#pub;

use std::collections::HashMap;

use jdb_base::{ckp::sst::Sst, sst::Level};

use super::{LevelSize, find_base_level, level_target_size};
use crate::{Id, LEVEL_LEN_MINUS_1};

/// Score scale: 128 represents 100%
/// 评分基准：128 代表 100%
pub const SCALE: u64 = 128;

/// L0 max file count to trigger compaction
/// L0 触发压缩的最大文件数
pub const L0_MAX_FILE_NUMBER: u64 = 4;

pub type LevelScore = u32;
pub type FileScore = u32;

/// Calculate sink score for L0
/// 计算 L0 的下沉得分
#[inline]
pub fn l0(count: u64) -> LevelScore {
  if count > L0_MAX_FILE_NUMBER * 2 {
    return LevelScore::MAX;
  }
  (count.saturating_mul(SCALE) / L0_MAX_FILE_NUMBER) as LevelScore
}

/// Calculate sink score for L1+
/// 计算 L1+ 的下沉得分
#[inline]
pub fn level_score(actual: u64, target: u64) -> LevelScore {
  if target == 0 {
    return if actual > 0 { LevelScore::MAX } else { 0 };
  }
  let raw = actual.saturating_mul(SCALE) / target;
  if raw > LevelScore::MAX as u64 {
    // 让 level 0 下沉更加优先
    LevelScore::MAX - 1
  } else {
    raw as LevelScore
  }
}

/// Calculate GC score for a single SST file (tombstone ratio)
/// 计算单个 SST 文件的 GC 得分（墓碑比例）
#[inline]
pub fn file_score(sst: &Sst) -> FileScore {
  if sst.size == 0 {
    return FileScore::MAX;
  }
  let raw = sst.rmed.saturating_mul(SCALE) / sst.size;
  if raw > FileScore::MAX as u64 {
    FileScore::MAX
  } else {
    raw as FileScore
  }
}

/// Level scoring state
/// 层级评分状态
#[derive(Debug)]
pub struct Score {
  pub total_size: u64,
  pub l0_cnt: u64,
  pub level_size: LevelSize,
  pub score: [LevelScore; LEVEL_LEN_MINUS_1],
  pub level_target_size: LevelSize,
  /// Target level for L0 compaction
  /// L0 压缩的目标层级
  pub base_level: Level,
  /// Files per level sorted by score descending
  /// 每层按得分降序排列的文件列表
  pub level_files: [Vec<(Id, FileScore)>; LEVEL_LEN_MINUS_1],
  /// Id to Sst mapping
  /// ID 到 Sst 的映射
  pub id_sst: HashMap<Id, Sst>,
  /// Recomputation needed
  /// 需要重新计算
  dirty: bool,
}

impl Score {
  /// Add batch SSTs
  /// 批量添加 SST
  fn push_iter(&mut self, iter: impl IntoIterator<Item = (Id, Sst)>) {
    iter.into_iter().for_each(|(id, sst)| {
      self.add(id, &sst);
      self.id_sst.insert(id, sst);
    });
  }

  /// Add SST to state (internal)
  /// 将 SST 添加到状态中（内部）
  #[inline]
  pub(super) fn add(&mut self, id: Id, sst: &Sst) {
    self.dirty = true;
    let size = sst.virtual_size();
    self.total_size = self.total_size.saturating_add(size);

    if sst.level == Level::L0 {
      self.l0_cnt = self.l0_cnt.saturating_add(1);
    } else {
      let idx = sst.level as usize - 1;
      // SAFETY: Level L1-L6 maps to index 0-5
      // 安全：Level L1-L6 对应索引 0-5
      unsafe {
        let lsize = self.level_size.get_unchecked_mut(idx);
        *lsize = lsize.saturating_add(size);

        let fs = file_score(sst);
        let files = self.level_files.get_unchecked_mut(idx);
        let pos = files.partition_point(|&(_, s)| s > fs);
        files.insert(pos, (id, fs));
      }
    }
  }

  /// Remove SST from state (internal)
  /// 从状态中移除 SST（内部）
  #[inline]
  pub(super) fn rm_sst(&mut self, id: Id, sst: &Sst) {
    self.dirty = true;
    let size = sst.virtual_size();
    self.total_size = self.total_size.saturating_sub(size);

    if sst.level == Level::L0 {
      self.l0_cnt = self.l0_cnt.saturating_sub(1);
    } else {
      let idx = sst.level as usize - 1;
      // SAFETY: Level L1-L6 maps to index 0-5
      // 安全：Level L1-L6 对应索引 0-5
      unsafe {
        let lsize = self.level_size.get_unchecked_mut(idx);
        *lsize = lsize.saturating_sub(size);

        let files = self.level_files.get_unchecked_mut(idx);
        if let Some(pos) = files.iter().position(|&(fid, _)| fid == id) {
          files.remove(pos);
        }
      }
    }
  }

  /// Recompute scores (cold path)
  /// 重新计算分数（冷路径）
  fn update(&mut self) {
    if !self.dirty {
      return;
    }
    let (target, base) = level_target_size(self.total_size);
    self.level_target_size = target;
    self.base_level = find_base_level(base, &self.level_size);

    // SAFETY: LEVEL_LEN_MINUS_1=6, index access is safe
    // 安全：LEVEL_LEN_MINUS_1=6，索引访问安全
    unsafe {
      *self.score.get_unchecked_mut(0) = l0(self.l0_cnt);
    }

    for (i, (&actual, &target)) in self
      .level_size
      .iter()
      .zip(&self.level_target_size)
      .take(LEVEL_LEN_MINUS_1 - 1)
      .enumerate()
    {
      // SAFETY: i is in 0..5, i+1 is in 1..6
      // 安全：i 在 0..5，i+1 在 1..6
      unsafe {
        *self.score.get_unchecked_mut(i + 1) = level_score(actual, target);
      }
    }
    self.dirty = false;
  }
}
