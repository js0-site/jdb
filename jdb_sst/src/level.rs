//! SSTable level type aliases
//! SSTable 层级类型别名

use jdb_level::{Level, Levels};

use crate::Table;

/// SSTable Level
/// SSTable 层级
pub type SstLevel = Level<Table>;

/// SSTable Levels
/// SSTable 层级管理器
pub type SstLevels = Levels<Table>;
