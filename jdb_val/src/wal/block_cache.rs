//! Block read cache for WAL files
//! WAL 文件的块读取缓存

use hashlink::LruCache;
use hipstr::HipByt;

use super::consts::{BLOCK_CACHE_CAP, BLOCK_SIZE};

/// Block cache key: (file_id, block_idx)
/// 块缓存键：(文件ID, 块索引)
type BlockKey = (u64, u64);

/// Block cache value (HipByt for small-string optimization/zero-copy)
/// 块缓存值（HipByt 优化小字符串/零拷贝）
type BlockData = HipByt<'static>;

/// Block read cache (LRU)
/// 块读取缓存（LRU）
pub struct BlockCache {
  cache: LruCache<BlockKey, BlockData>,
}

impl BlockCache {
  /// Create new block cache
  /// 创建新的块缓存
  pub fn new() -> Self {
    Self {
      cache: LruCache::new(BLOCK_CACHE_CAP),
    }
  }

  /// Get block index and offset for position
  /// 获取位置对应的块索引和偏移
  #[inline]
  pub fn block_idx(pos: u64) -> (u64, usize) {
    // Compiler optimizes to bitwise ops if BLOCK_SIZE is power of 2
    // 若 BLOCK_SIZE 为 2 的幂，编译器会自动优化为位运算
    (pos / BLOCK_SIZE as u64, (pos % BLOCK_SIZE as u64) as usize)
  }

  /// Check if range fits in single block
  /// 检查范围是否在单个块内
  #[inline]
  pub fn fits_in_block(off: usize, len: usize) -> bool {
    off + len <= BLOCK_SIZE
  }

  /// Get cached block
  /// 获取缓存的块
  #[inline]
  pub fn get(&mut self, fid: u64, blk_idx: u64) -> Option<HipByt<'static>> {
    // HipByt clone is cheap (atomic increment or inline copy)
    // HipByt 克隆开销很低（原子递增或内联复制）
    self.cache.get(&(fid, blk_idx)).cloned()
  }

  /// Insert block into cache
  /// 插入块到缓存
  #[inline]
  pub fn insert(&mut self, fid: u64, blk_idx: u64, data: HipByt<'static>) {
    self.cache.insert((fid, blk_idx), data);
  }
}

impl Default for BlockCache {
  fn default() -> Self {
    Self::new()
  }
}
