//! WAL constants
//! WAL 常量

use crate::INFILE_MAX;

// === Header / 文件头 ===

/// WAL file header size (12B)
/// WAL 文件头大小
pub const HEADER_SIZE: usize = 12;

/// Minimum valid WAL file size
/// 最小有效 WAL 文件大小
pub const MIN_FILE_SIZE: u64 = HEADER_SIZE as u64;

/// Current version
/// 当前版本
pub const WAL_VERSION: u32 = 1;

// === Record / 记录 ===

/// Scan buffer size (1MB + 64KB, covers max infile)
/// 扫描缓冲区大小
pub const SCAN_BUF_SIZE: usize = INFILE_MAX + 64 * 1024;

/// Iterator buffer size (64KB)
/// 迭代器缓冲区大小
pub const ITER_BUF_SIZE: usize = 64 * 1024;

/// Small read buffer size (64KB, covers most medium records in one IO)
/// 小读取缓冲区大小（64KB，一次 IO 覆盖大多数中等记录）
pub const SMALL_BUF_SIZE: usize = 64 * 1024;

// === Defaults / 默认值 ===

/// Default max file size (512MB)
/// 默认最大文件大小
pub const DEFAULT_MAX_SIZE: u64 = 512 * 1024 * 1024;

/// Default write queue capacity
/// 默认写入队列容量
pub const DEFAULT_WRITE_CHAN: usize = 4096;

/// Default cache size (8MB)
/// 默认缓存大小
pub const DEFAULT_CACHE_SIZE: u64 = 8 * 1024 * 1024;

/// Min cache size (1MB)
/// 最小缓存大小
pub const MIN_CACHE_SIZE: u64 = 1024 * 1024;

/// Block cache ratio (10% of total cache)
/// 块缓存比例（总缓存的 10%）
pub const BLOCK_CACHE_RATIO: u64 = 10;

/// Default file cache capacity
/// 默认文件缓存容量
pub const DEFAULT_FILE_CAP: usize = 64;

/// Min file cache capacity
/// 最小文件缓存容量
pub const MIN_FILE_CAP: usize = 4;

/// Default BIN file cache capacity
/// 默认 BIN 文件缓存容量
pub const DEFAULT_BIN_CAP: usize = 16;

/// Min BIN file cache capacity
/// 最小 BIN 文件缓存容量
pub const MIN_BIN_CAP: usize = 2;

/// Default slot max size (8MB, ~5ms+ write time on fast SSD)
/// 默认槽最大大小（8MB，快速 SSD 上约 5ms+ 写入时间）
pub const DEFAULT_SLOT_MAX: usize = 8 * 1024 * 1024;

// === Paths / 路径 ===

/// WAL subdirectory name
/// WAL 子目录名
pub const WAL_SUBDIR: &str = "wal";

/// Bin subdirectory name
/// Bin 子目录名
pub const BIN_SUBDIR: &str = "bin";

/// Lock directory name
/// 锁目录名
pub const LOCK_SUBDIR: &str = "lock";

/// WAL lock type (for GC)
/// WAL 锁类型（用于 GC）
pub const WAL_LOCK_TYPE: &str = "wal";

/// GC temp directory name
/// GC 临时目录名
pub const GC_SUBDIR: &str = "gc";
