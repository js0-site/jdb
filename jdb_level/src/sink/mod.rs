mod level_size;
mod score;

use jdb_base::sst::Level;
use level_size::{find_base_level, level_target_size};
pub use score::Score;

use crate::{Id, LEVEL_LEN_MINUS_1};

/// Size array for each level (L1-L6)
/// 每层大小数组 (L1-L6)
pub type LevelSize = [u64; LEVEL_LEN_MINUS_1];

// 下沉的目标层级
pub type ToLevel = Level;

pub enum Sink {
  L0(ToLevel),
  L1Plus { from: Level, to: ToLevel, id: Id },
}
