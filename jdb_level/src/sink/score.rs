use jdb_base::{ckp::Sst, sst::Level};

use super::{LevelSize, N, adjust_base_level, level_target_size};

/// Score scale: 1000 represents 100%
/// 评分基准：1000 代表 100%
pub const SCALE: u64 = 1000;

/// L0 max file count to trigger compaction
/// L0 触发压缩的最大文件数
pub const L0_MAX_FILE_NUMBER: u64 = 4;

/// Type aliases for clarity
/// 类型别名以提高清晰度
pub type Id = u64;
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
  let raw = actual / (1 + target / SCALE);
  if raw > LevelScore::MAX as u64 {
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
    return 0;
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
  pub score: [LevelScore; N],
  pub level_target_size: LevelSize,
  /// Target level for L0 compaction
  /// L0 压缩的目标层级
  pub base_level: Level,
  /// Files per level sorted by score descending
  /// 每层按得分降序排列的文件列表
  pub level_files: [Vec<(Id, FileScore)>; N],
  /// Id to Sst mapping
  /// ID 到 Sst 的映射
  id_sst: std::collections::HashMap<Id, Sst>,
  /// Recomputation needed
  /// 需要重新计算
  dirty: bool,
}

/// GC metadata
/// GC 元数据
pub struct GcMeta {
  pub from: Level,
  pub to: Level,
  pub id: Id,
}

/// GC decision result
/// GC 决策结果
pub enum Gc {
  L0(Level),
  L1Plus(GcMeta),
}

impl Score {
  /// Create new Score from SSTs
  /// 从 SST 列表创建新评分
  pub fn new(iter: impl IntoIterator<Item = (Id, Sst)>) -> Self {
    let mut score = Self {
      level_target_size: [0; N],
      score: [0; N],
      total_size: 0,
      l0_cnt: 0,
      level_size: [0; N],
      base_level: Level::L6,
      level_files: Default::default(),
      id_sst: Default::default(),
      dirty: true,
    };
    score.add(iter);
    score
  }

  /// Get next GC target (triggers lazy computation)
  /// 获取下一个 GC 目标（触发延迟计算）
  pub fn next_gc(&mut self) -> Option<Gc> {
    self.recalc_scores();

    let (idx, &max) = self.score.iter().enumerate().max_by_key(|&(_, s)| s)?;
    if max < SCALE as LevelScore {
      return None;
    }

    // SAFETY: idx < N, Level conversion is safe
    // 安全：idx < N，Level 转换安全
    let from: Level = unsafe { std::mem::transmute(idx as u8) };

    if from == Level::L0 {
      return Some(Gc::L0(self.base_level));
    }

    let to = if from < self.base_level {
      self.base_level
    } else {
      from.next().unwrap_or(Level::L6)
    };

    // SAFETY: from is L1-L5, idx = from-1 is 0-4
    // 安全：from 是 L1-L5，idx = from-1 为 0-4
    let files = unsafe { self.level_files.get_unchecked(from as usize - 1) };
    let &(id, _) = files.first()?;

    Some(Gc::L1Plus(GcMeta { from, to, id }))
  }

  /// Add a single SST
  /// 添加单个 SST
  #[inline]
  pub fn push(&mut self, id: Id, sst: Sst) {
    self.add_sst(id, &sst);
    self.id_sst.insert(id, sst);
  }

  /// Add batch SSTs
  /// 批量添加 SST
  fn add(&mut self, iter: impl IntoIterator<Item = (Id, Sst)>) {
    for (id, sst) in iter {
      self.add_sst(id, &sst);
      self.id_sst.insert(id, sst);
    }
  }

  /// Remove batch SSTs by ID
  /// 通过 ID 批量移除 SST
  pub fn rm<'a>(&mut self, iter: impl IntoIterator<Item = (Level, &'a [Id])>) {
    for (level, ids) in iter {
      for &id in ids {
        if let Some(sst) = self.id_sst.remove(&id) {
          debug_assert_eq!(sst.level, level);
          self.rm_sst(id, &sst);
        }
      }
    }
  }

  /// Add SST to state (internal)
  /// 将 SST 添加到状态中（内部）
  #[inline]
  fn add_sst(&mut self, id: Id, sst: &Sst) {
    self.dirty = true;
    let size = sst.size_without_rmed();
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
  fn rm_sst(&mut self, id: Id, sst: &Sst) {
    self.dirty = true;
    let size = sst.size_without_rmed();
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
  #[cold]
  fn recalc_scores(&mut self) {
    if !self.dirty {
      return;
    }
    let (target, base) = level_target_size(self.total_size);
    self.level_target_size = target;
    self.base_level = adjust_base_level(base, &self.level_size);

    // SAFETY: N=6, index access is safe
    // 安全：N=6，索引访问安全
    unsafe {
      *self.score.get_unchecked_mut(0) = l0(self.l0_cnt);
    }

    for (i, (&actual, &target)) in self
      .level_size
      .iter()
      .zip(&self.level_target_size)
      .take(N - 1)
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

  /// Update state and recalc
  /// 更新状态并重新计算
  pub fn update<'a>(
    &mut self,
    add: impl IntoIterator<Item = (Id, Sst)>,
    rm: impl IntoIterator<Item = (Level, &'a [Id])>,
  ) {
    self.add(add);
    self.rm(rm);
  }
}
