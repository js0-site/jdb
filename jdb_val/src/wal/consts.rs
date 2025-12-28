//! WAL constants / WAL 常量

use crate::{Head, INFILE_MAX};

// === Magic / 魔数 ===

/// Record magic value / 记录魔数值
pub const MAGIC: u32 = 0xED_ED_ED_ED;

/// Magic bytes / 魔数字节
pub const MAGIC_BYTES: [u8; 4] = MAGIC.to_le_bytes();

/// Magic size / 魔数大小
pub const MAGIC_SIZE: usize = 4;

// === Header / 文件头 ===

/// WAL file header size (12B) / WAL 文件头大小
pub const HEADER_SIZE: usize = 12;

/// Minimum valid WAL file size / 最小有效 WAL 文件大小
pub const MIN_FILE_SIZE: u64 = HEADER_SIZE as u64;

/// Current version / 当前版本
pub const WAL_VERSION: u32 = 1;

// === Record / 记录 ===

/// Record header size: Magic(4) + Head(64) = 68
/// 记录头大小
pub const RECORD_HEADER_SIZE: usize = MAGIC_SIZE + Head::SIZE;

/// Scan buffer size (1MB + 64KB, covers max infile) / 扫描缓冲区大小
pub const SCAN_BUF_SIZE: usize = INFILE_MAX + 64 * 1024;

/// Iterator buffer size (64KB) / 迭代器缓冲区大小
pub const ITER_BUF_SIZE: usize = 64 * 1024;

// === Defaults / 默认值 ===

/// Default max file size (256MB) / 默认最大文件大小
pub const DEFAULT_MAX_SIZE: u64 = 256 * 1024 * 1024;

/// Default write queue capacity / 默认写入队列容量
pub const DEFAULT_WRITE_CHAN: usize = 4096;

/// Default cache size (8MB) / 默认缓存大小
pub const DEFAULT_CACHE_SIZE: u64 = 8 * 1024 * 1024;

/// Default file cache capacity / 默认文件缓存容量
pub const DEFAULT_FILE_CAP: usize = 64;

// === Cache Memory Layout / 缓存内存布局 ===
// Total cache split: 70% head_cache, 30% data_cache
// 总缓存分配：70% head_cache，30% data_cache

/// Head entry size: Pos(16) + Head(64) + LRU overhead(~48) ≈ 128B
/// Head 条目大小：Pos(16) + Head(64) + LRU 开销(~48) ≈ 128B
pub const HEAD_ENTRY_SIZE: usize = 128;

/// Data entry avg size: Pos(16) + Rc(16) + avg_data(~480) + LRU overhead(~48) ≈ 512B
/// Data 条目平均大小：Pos(16) + Rc(16) + 平均数据(~480) + LRU 开销(~48) ≈ 512B
pub const DATA_ENTRY_AVG_SIZE: usize = 512;

/// Head cache ratio (70%) / Head 缓存比例
/// Head is hot path, accessed on every read / Head 是热点路径，每次读取都访问
pub const HEAD_CACHE_RATIO: u64 = 70;

/// Data cache ratio (30%) / Data 缓存比例
/// Only INFILE mode uses data cache / 仅 INFILE 模式使用数据缓存
pub const DATA_CACHE_RATIO: u64 = 30;

// === Paths / 路径 ===

/// WAL subdirectory name / WAL 子目录名
pub const WAL_SUBDIR: &str = "wal";

/// Bin subdirectory name / Bin 子目录名
pub const BIN_SUBDIR: &str = "bin";

/// Lock directory name / 锁目录名
pub const LOCK_SUBDIR: &str = "lock";

/// WAL lock type (for GC) / WAL 锁类型（用于 GC）
pub const WAL_LOCK_TYPE: &str = "wal";

/// Calculate cache capacities from total size / 根据总大小计算缓存容量
/// Returns (head_cap, data_cap), min 1 for LruCache / 返回 (head_cap, data_cap)，LruCache 最小为 1
#[inline]
pub fn calc_cache_cap(total_bytes: u64) -> (usize, usize) {
  if total_bytes == 0 {
    return (1, 1); // LruCache requires cap >= 1 / LruCache 要求容量 >= 1
  }
  let head_bytes = total_bytes * HEAD_CACHE_RATIO / 100;
  let data_bytes = total_bytes * DATA_CACHE_RATIO / 100;
  let head_cap = (head_bytes as usize / HEAD_ENTRY_SIZE).max(1);
  let data_cap = (data_bytes as usize / DATA_ENTRY_AVG_SIZE).max(1);
  (head_cap, data_cap)
}
