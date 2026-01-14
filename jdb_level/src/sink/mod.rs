mod level_size;
mod score;

use jdb_base::sst::Level;
use level_size::{adjust_base_level, level_target_size};
pub use score::Score;

use crate::Id;
/// Number of levels excluding L0 (L1-L6)
/// 排除 L0 的层级数量 (L1-L6)
pub const N: usize = jdb_base::sst::Level::LEN - 1;

/// Size array for each level (L1-L6)
/// 每层大小数组 (L1-L6)
pub type LevelSize = [u64; N];

// 下沉的目标层级
pub type ToLevel = Level;

pub enum Sink {
  L0(ToLevel),
  L1Plus { from: Level, to: ToLevel, id: Id },
}

/// Score scale: 128 represents 100%
/// 评分基准：128 代表 100%
pub const SCALE: u64 = 128;

/// L0 max file count to trigger compaction
/// L0 触发压缩的最大文件数
pub const L0_MAX_FILE_NUMBER: u64 = 4;

pub type LevelScore = u32;
pub type FileScore = u32;
