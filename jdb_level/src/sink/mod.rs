mod level_size;
mod score;

pub use level_size::{adjust_base_level, level_target_size};
pub use score::{FileScore, Gc, Id, LevelScore, Score, file_score, l0, level_score};

/// Number of levels excluding L0 (L1-L6)
/// 排除 L0 的层级数量 (L1-L6)
pub const N: usize = jdb_base::sst::Level::LEN - 1;

/// Size array for each level (L1-L6)
/// 每层大小数组 (L1-L6)
pub type LevelSize = [u64; N];
