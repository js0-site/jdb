//! SSTable level re-exports
//! SSTable 层级重导出

pub use jdb_level::{Conf, Level, Levels, conf, new};

use crate::Table;

/// SSTable Level
/// SSTable 层级
pub type SstLevel = Level<Table>;

/// SSTable Levels
/// SSTable 多层管理器
pub type SstLevels = Levels<Table>;
