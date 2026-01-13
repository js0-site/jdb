mod level_size;
mod score;
pub use level_size::level_size;

pub const N: usize = jdb_base::sst::Level::LEN - 1;
pub type LevelSize = [u64; N];
