//! WAL constants / WAL 常量

use crate::Head;

// === End Marker / 尾部标记 ===

/// End marker size (12B) / 尾部标记大小
pub const END_SIZE: usize = 12;

/// Magic value / 魔数值
pub const END_MAGIC: u32 = 0xED_ED_ED_ED;

/// Magic bytes for search / 搜索用魔数字节
pub const MAGIC_BYTES: [u8; 4] = [0xED, 0xED, 0xED, 0xED];

// === Header / 文件头 ===

/// WAL file header size (12B) / WAL 文件头大小
pub const HEADER_SIZE: usize = 12;

/// Current version / 当前版本
pub const WAL_VERSION: u32 = 1;

// === Recovery / 恢复 ===

/// Scan buffer size (64KB) / 扫描缓冲区大小
pub const SCAN_BUF_SIZE: usize = 64 * 1024;

/// Min file size for fast recovery / 快速恢复最小文件大小
/// Header(12) + Head(64) + End(12) = 88
pub const MIN_FAST_SIZE: u64 = (HEADER_SIZE + Head::SIZE + END_SIZE) as u64;

// === Defaults / 默认值 ===

/// Default max file size (256MB) / 默认最大文件大小
pub const DEFAULT_MAX_SIZE: u64 = 256 * 1024 * 1024;

/// Default write channel capacity / 默认写入通道容量
pub const DEFAULT_WRITE_CHAN: usize = 8192;

/// Default head cache capacity / 默认头缓存容量
pub const DEFAULT_HEAD_CAP: usize = 8192;

/// Default data cache capacity / 默认数据缓存容量
pub const DEFAULT_DATA_CAP: usize = 1024;

/// Default file cache capacity / 默认文件缓存容量
pub const DEFAULT_FILE_CAP: usize = 64;

// === Paths / 路径 ===

/// WAL subdirectory name / WAL 子目录名
pub const WAL_SUBDIR: &str = "wal";

/// Bin subdirectory name / Bin 子目录名
pub const BIN_SUBDIR: &str = "bin";
