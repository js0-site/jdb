use jdb_base::sst::Level;

use super::{LevelSize, N};

/// Base level target size (256 MB)
/// 基础层的目标大小 (256 MB)
const BASE_SIZE: u64 = 256 * 1024 * 1024;

/// Size multiplier per level
/// 每一层的放大倍数
const SCALE: u64 = 8;

/// Calculate target size for each level based on total data size
/// Returns (LevelSize, base_level)
/// 根据总数据大小计算每层目标大小
/// 返回 (LevelSize, base_level)
#[inline]
pub fn level_target_size(total: u64) -> (LevelSize, Level) {
  let mut li = [0u64; N];
  let mut idx = N - 1;
  let mut target = total;

  for i in (0..N).rev() {
    // Optimization: If target < BASE_SIZE and not the last level (L6), stop early.
    // This ensures Base Level >= BASE_SIZE to buffer L0 data effectively.
    // 优化：如果目标小于 BASE_SIZE 且不是最后一层（L6），提早停止。
    // 这确保留基础层 >= BASE_SIZE，以有效缓冲 L0 数据。
    if target < BASE_SIZE && i < N - 1 {
      break;
    }

    // SAFETY: i is in 0..N, li has size N
    // 安全：i 在 0..N 范围内，li 大小为 N
    unsafe {
      *li.get_unchecked_mut(i) = target;
    }
    idx = i;

    target /= SCALE;
  }

  // Convert index to Level: 0..6 -> L1..L6
  // SAFETY: idx + 1 is in 1..=6, which is valid for Level
  // 将索引转换为 Level：0..6 -> L1..L6
  // 安全：idx + 1 在 1..=6 范围内，是有效的 Level
  let level: Level = unsafe { std::mem::transmute::<u8, Level>((idx + 1) as u8) };

  (li, level)
}

// 找到第一个实际大小不为0的层作为base_level
#[inline]
pub fn find_base_level(target_base: Level, actual: &LevelSize) -> Level {
  let target_idx = target_base as usize - 1;

  for i in 0..target_idx {
    // SAFETY: i < target_idx <= 5, actual has size 6
    // 安全：i < target_idx <= 5，actual 大小为 6
    unsafe {
      if *actual.get_unchecked(i) > 0 {
        return std::mem::transmute::<u8, Level>((i + 1) as u8);
      }
    }
  }

  target_base
}
