//! Dynamic level bytes calculation
//! 动态层级字节计算

use crate::conf::{MAX_LEVEL, MAX_LEVELS};

/// Score scale factor for precision
/// 评分缩放因子以保持精度
pub const SCORE_SCALE: u32 = 100;

/// Score threshold for urgent compaction (levels above base_level)
/// 紧急压缩评分阈值（base_level 之上的层级）
pub const SCORE_URGENT: u32 = 200;

/// Dynamic level limits result
/// 动态层级限制结果
#[derive(Debug, Clone, Copy)]
pub struct Limits {
  pub limits: [u64; MAX_LEVELS],
  pub base_level: u8,
}

impl Default for Limits {
  fn default() -> Self {
    Self {
      limits: [0; MAX_LEVELS],
      base_level: MAX_LEVEL,
    }
  }
}

/// Calculate dynamic level limits (RocksDB style)
/// 计算动态层级限制（RocksDB 风格）
#[inline]
pub fn calc(total: u64, base_size: u64, ratio: u64) -> Limits {
  let mut r = Limits::default();

  // L0 has no size limit
  // L0 无大小限制
  r.limits[0] = u64::MAX;

  if total == 0 {
    // Empty: base_level = MAX_LEVEL, limit = base_size
    // 空：base_level = MAX_LEVEL，limit = base_size
    r.limits[MAX_LEVEL as usize] = base_size;
    return r;
  }

  // Find base_level: highest level where limit >= base_size
  // 找 base_level：limit >= base_size 的最高层
  let mut base_level = MAX_LEVEL;
  let mut limit = total.max(base_size);

  for level in (1..MAX_LEVEL).rev() {
    let next = limit / ratio;
    if next < base_size {
      break;
    }
    base_level = level;
    limit = next;
  }

  r.base_level = base_level;

  // Set limits from base_level down
  // 从 base_level 往下设置 limits
  let mut cur = limit;
  for lim in &mut r.limits[base_level as usize..] {
    *lim = cur;
    cur = cur.saturating_mul(ratio);
  }

  r
}

/// Calculate compaction score for a level (RocksDB style)
/// 计算层级的压缩评分（RocksDB 风格）
/// Uses compensated_size (file_size - rmed_size) for better tombstone handling
/// 使用补偿大小（file_size - rmed_size）以更好处理墓碑
/// Returns score * 100 as integer for precision
/// 返回 score * 100 的整数以保持精度
#[inline]
pub fn score(
  level: u8,
  level_len: usize,
  compensated_size: u64,
  l0_limit: usize,
  limit: u64,
) -> u32 {
  if level >= MAX_LEVEL {
    return 0;
  }
  if level == 0 {
    // L0: files_count * 100 / l0_limit
    // L0：文件数 * 100 / l0_limit
    (level_len as u32).saturating_mul(SCORE_SCALE) / l0_limit.max(1) as u32
  } else if limit > 0 {
    // L1+: compensated_size * 100 / target_size
    // L1+：补偿大小 * 100 / 目标大小
    (compensated_size.saturating_mul(SCORE_SCALE as u64) / limit) as u32
  } else {
    0
  }
}

/// Check if level needs compaction (score > SCORE_SCALE)
/// 检查层级是否需要压缩（score > SCORE_SCALE）
#[inline]
pub fn needs_compact(
  level: u8,
  level_len: usize,
  compensated_size: u64,
  l0_limit: usize,
  base_level: u8,
  limit: u64,
) -> bool {
  if level >= MAX_LEVEL {
    return false;
  }
  if level == 0 {
    level_len >= l0_limit
  } else if level < base_level {
    // Levels above base_level: compact if not empty
    // base_level 以上的层：非空就需要压缩
    level_len > 0
  } else {
    compensated_size > limit
  }
}

/// Get target level for compaction
/// 获取压缩目标层
#[inline]
pub fn target_level(src: u8, base_level: u8) -> u8 {
  if src == 0 {
    base_level
  } else {
    (src + 1).min(MAX_LEVEL)
  }
}
