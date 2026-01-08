//! SSTable level type aliases
//! SSTable 层级类型别名

use jdb_level::{Level, Levels};

use crate::{Handle, Table};

/// SSTable Level (Table)
/// SSTable 层级（Table）
pub type SstLevel = Level<Table>;

/// SSTable Levels (Table)
/// SSTable 层级管理器（Table）
pub type SstLevels = Levels<Table>;

/// Handle Level
/// Handle 层级
pub type HandleLevel = Level<Handle>;

/// Handle Levels
/// Handle 层级管理器
pub type HandleLevels = Levels<Handle>;
