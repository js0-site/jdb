use jdb_base::sst::Level;

use super::{LevelSize, N, level_size};

pub type Size = u64;

// 评分基准：100 代表 100%
pub const SCALE: u64 = 1000;

// L0 触发 Compaction 的最大文件数
pub const L0_MAX_FILE_NUMBER: u64 = 4;

/// 计算 Level 0 的下沉得分
/// Calculate sink score for Level 0
#[inline]
pub fn l0(count: u64) -> u32 {
  if count > L0_MAX_FILE_NUMBER * 2 {
    return u32::MAX;
  }
  ((count * SCALE) / L0_MAX_FILE_NUMBER) as u32
}

/// 计算 Level 1+ 的下沉得分 (保持不变)
/// Calculate sink score for Level 1+
#[inline]
pub fn score(current_size: u64, target_size: u64) -> u32 {
  if target_size == 0 {
    return if current_size > 0 { u32::MAX } else { 0 };
  }
  let raw_score = current_size / (1 + target_size / SCALE);

  if raw_score > u32::MAX as u64 {
    u32::MAX - 1
  } else {
    raw_score as u32
  }
}

pub struct Score {
  pub total_size: u64,
  pub l0_cnt: u64,
  pub level_size: LevelSize,
  pub score: [u32; N],
  pub level_target_size: LevelSize,
}

// 中间有期望大小为0的层，快速跳层
// Skip intermediate levels with 0 target size
pub struct FromTo {
  pub from: Level,
  pub to: Level,
}

impl Score {
  pub fn new(iter: impl IntoIterator<Item = (Level, Size)>) -> Self {
    let mut score = Self {
      level_target_size: [0; N],
      score: [0; N],
      total_size: 0,
      l0_cnt: 0,
      level_size: [0; N],
    };
    score.update(iter, []);
    score
  }

  pub fn next_gc_level(&self) -> Option<FromTo> {
    // 寻找分数最高的层级
    // Find the level with the highest score
    // 当分数相同时，max_by_key 返回第一个最大值 (低层优先)。
    // 这是合理的，优先缓解 L0/L1 压力以避免 Write Stall。
    // When scores are equal, max_by_key returns the first max (lower level first).
    // This is reasonable to prioritize relieving L0/L1 pressure to avoid write stalls.
    let (idx, &max_score) = self.score.iter().enumerate().max_by_key(|&(_, s)| s)?;

    if max_score < SCALE as u32 {
      return None;
    }

    // Index i corresponds to Level i
    // SAFETY: idx is bounded by N=6, which is < Level::LEN=7
    let from: Level = unsafe { std::mem::transmute(idx as u8) };
    let mut to = from;

    while let Some(next) = to.next() {
      to = next;
      // 找到第一个期望大小 > 0 或 实际大小 > 0 的层级
      // Find the first level with target size > 0 or actual size > 0
      // L1-L6 对应 level_target_size 索引 0-5
      // L1-L6 maps to level_target_size index 0-5
      // SAFETY:
      // 1. `to` starts from `from` (>= L0) + 1 => min L1.
      // 2. `next()` ensures `to` <= L6.
      // 3. `to as usize` is in [1, 6].
      // 4. `idx = to as usize - 1` is in [0, 5].
      // 5. Array size N is 6. Access is safe.
      let idx = (to as usize) - 1;
      unsafe {
        if *self.level_target_size.get_unchecked(idx) > 0 || *self.level_size.get_unchecked(idx) > 0
        {
          break;
        }
      }
    }

    Some(FromTo { from, to })
  }

  pub fn update(
    &mut self,
    add_iter: impl IntoIterator<Item = (Level, Size)>,
    rm_iter: impl IntoIterator<Item = (Level, Size)>,
  ) {
    for (level, size) in add_iter {
      self.total_size += size;
      if level == Level::L0 {
        self.l0_cnt += 1;
      } else {
        // SAFETY: If level is not L0, it is between L1 (1) and L6 (6).
        // -1 gives 0..=5. level_size len is N=6.
        unsafe {
          *self.level_size.get_unchecked_mut(level as usize - 1) += size;
        }
      }
    }

    for (level, size) in rm_iter {
      self.total_size -= size;
      if level == Level::L0 {
        self.l0_cnt -= 1;
      } else {
        // SAFETY: Same as above.
        unsafe {
          *self.level_size.get_unchecked_mut(level as usize - 1) -= size;
        }
      }
    }

    self.level_target_size = level_size(self.total_size);

    // SAFETY: N=6 > 0.
    unsafe {
      *self.score.get_unchecked_mut(0) = l0(self.l0_cnt);
    }

    // L1-L5 (indices 1..5 in score)
    // L1-L5 (索引 1..5)
    for (i, (&size, &target)) in self
      .level_size
      .iter()
      .zip(&self.level_target_size)
      .take(N - 1)
      .enumerate()
    {
      // SAFETY:
      // i is 0..N-2 (0..4).
      // i + 1 is 1..N-1 (1..5).
      // score len is N (6).
      unsafe {
        *self.score.get_unchecked_mut(i + 1) = score(size, target);
      }
    }
  }
}
