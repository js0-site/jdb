//! WAL configuration
//! WAL 配置

use std::rc::Rc;

use jdb_lock::{NoLock, WalLock, w::Lock as WLock};
use size_lru::{Lhd, NoCache as SzNoCache, SizeLru};

use super::consts::{
  BLOCK_CACHE_RATIO, DEFAULT_BIN_CAP, DEFAULT_CACHE_SIZE, DEFAULT_FILE_CAP, DEFAULT_MAX_SIZE,
  DEFAULT_SLOT_MAX, DEFAULT_WRITE_CHAN, MIN_BIN_CAP, MIN_CACHE_SIZE, MIN_FILE_CAP,
};
use crate::{Flag, Pos};

/// GC trait for data processing during GC
/// GC 数据处理 trait
///
/// Process data during GC, may compress
/// GC 过程中处理数据，可能压缩
pub trait Gc: Send + 'static {
  /// Process data during GC
  /// GC 过程中处理数据
  ///
  /// Returns (new_store, data_slice_end)
  /// 返回 (新存储模式, 数据切片结束位置)
  /// - compressed data in buf[..end] or original
  /// - 压缩数据在 buf[..end] 或原始数据
  fn process(&mut self, store: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>);
}

/// Cached data type
/// 缓存数据类型
pub type CachedData = Rc<[u8]>;

/// WAL configuration trait
/// WAL 配置 trait
pub trait WalConf {
  type ValCache: SizeLru<Pos, CachedData>;
  type Lock: WalLock;

  /// Create cache and lock
  /// 创建缓存和锁
  fn create(conf: &ParsedConf) -> (Self::ValCache, Self::Lock);
}

/// Default WAL config (with LHD cache and lock)
/// 默认 WAL 配置（带 LHD 缓存和锁）
pub struct DefaultConf;

impl WalConf for DefaultConf {
  type ValCache = Lhd<Pos, CachedData>;
  type Lock = WLock;

  fn create(conf: &ParsedConf) -> (Self::ValCache, Self::Lock) {
    (Lhd::new(conf.cache_size as usize), WLock::default())
  }
}

/// GC WAL config (no cache, no lock)
/// GC WAL 配置（无缓存，无锁）
pub struct GcConf;

impl WalConf for GcConf {
  type ValCache = SzNoCache;
  type Lock = NoLock;

  fn create(_: &ParsedConf) -> (Self::ValCache, Self::Lock) {
    (SzNoCache, NoLock)
  }
}

/// WAL configuration options
/// WAL 配置选项
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Max WAL file size in bytes
  /// 最大文件大小（字节）
  MaxSize(u64),
  /// Total cache size in bytes (auto-split to head/data caches)
  /// 总缓存大小（字节），自动分配到 head/data 缓存
  CacheSize(u64),
  /// File handle cache capacity
  /// 文件句柄缓存容量
  FileLru(usize),
  /// Write queue capacity
  /// 写入队列容量
  WriteChan(usize),
  /// Max slot size before waiting
  /// 等待前的最大槽大小
  SlotMax(usize),
}

/// Parsed config
/// 解析后的配置
pub struct ParsedConf {
  pub max_size: u64,
  pub cache_size: u64,
  pub block_cache_size: u64,
  pub file_cap: usize,
  pub bin_cap: usize,
  pub write_chan: usize,
  pub slot_max: usize,
}

impl ParsedConf {
  pub fn parse(conf: &[Conf]) -> Self {
    let mut c = Self {
      max_size: DEFAULT_MAX_SIZE,
      cache_size: DEFAULT_CACHE_SIZE,
      block_cache_size: 0,
      file_cap: DEFAULT_FILE_CAP,
      bin_cap: DEFAULT_BIN_CAP,
      write_chan: DEFAULT_WRITE_CHAN,
      slot_max: DEFAULT_SLOT_MAX,
    };
    for item in conf {
      match *item {
        Conf::MaxSize(v) => c.max_size = v,
        Conf::CacheSize(v) => c.cache_size = v,
        Conf::FileLru(v) => c.file_cap = v,
        Conf::WriteChan(v) => c.write_chan = v,
        Conf::SlotMax(v) => c.slot_max = v,
      }
    }
    c.cache_size = c.cache_size.max(MIN_CACHE_SIZE);
    c.block_cache_size = c.cache_size * BLOCK_CACHE_RATIO / 100;
    c.file_cap = c.file_cap.max(MIN_FILE_CAP);
    c.bin_cap = c.bin_cap.max(MIN_BIN_CAP);
    c
  }
}
