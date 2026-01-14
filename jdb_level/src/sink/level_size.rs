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
    // SAFETY: i is in 0..N, li has size N
    // 安全：i 在 0..N 范围内，li 大小为 N
    unsafe {
      *li.get_unchecked_mut(i) = target;
    }
    idx = i;

    // Data goes directly to base level if smaller than BASE_SIZE
    // 如果小于 BASE_SIZE，数据直接进入基础层
    if target <= BASE_SIZE {
      break;
    }
    target /= SCALE;
  }

  // Convert index to Level: 0..6 -> L1..L7 (L7 is L6.next())
  // SAFETY: idx + 1 is in 1..=6, which is valid for Level
  // 将索引转换为 Level：0..6 -> L1..L7
  // 安全：idx + 1 在 1..=6 范围内，是有效的 Level
  let level: Level = unsafe { std::mem::transmute::<u8, Level>((idx + 1) as u8) };

  (li, level)
}

/// Adjust base level considering actual level sizes
/// Use the lowest level that has data as base
/// 根据实际层大小调整 base level
/// 使用有数据的最低层作为基础层
#[inline]
pub fn adjust_base_level(target_base: Level, actual: &LevelSize) -> Level {
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
