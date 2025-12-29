//! WAL configuration
//! WAL 配置

use std::rc::Rc;

use jdb_lock::{NoLock, WalLock, w::Lock as WLock};
use jdb_lru::{Cache, Lru, NoCache};
use size_lru::{Lhd, NoCache as SzNoCache, SizeLru};

use super::consts::{
  DEFAULT_CACHE_SIZE, DEFAULT_FILE_CAP, DEFAULT_MAX_SIZE, DEFAULT_WRITE_CHAN, calc_cache_cap,
};
use crate::{Head, Pos, Store};

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
  fn process(&mut self, store: Store, data: &[u8], buf: &mut Vec<u8>) -> (Store, Option<usize>);
}

/// Cached data type
/// 缓存数据类型
pub type CachedData = Rc<[u8]>;

/// WAL configuration trait
/// WAL 配置 trait
pub trait WalConf {
  type HeadCache: Cache<Pos, Head>;
  type DataCache: SizeLru<Pos, CachedData>;
  type Lock: WalLock;

  /// Create caches and lock
  /// 创建缓存和锁
  fn create(conf: &ParsedConf) -> (Self::HeadCache, Self::DataCache, Self::Lock);
}

/// Default WAL config (with LHD cache and lock)
/// 默认 WAL 配置（带 LHD 缓存和锁）
pub struct DefaultConf;

impl WalConf for DefaultConf {
  type HeadCache = Lru<Pos, Head>;
  type DataCache = Lhd<Pos, CachedData>;
  type Lock = WLock;

  fn create(conf: &ParsedConf) -> (Self::HeadCache, Self::DataCache, Self::Lock) {
    let (head_cap, data_bytes) = calc_cache_cap(conf.cache_size);
    (Lru::new(head_cap), Lhd::new(data_bytes), WLock::default())
  }
}

/// GC WAL config (no cache, no lock)
/// GC WAL 配置（无缓存，无锁）
pub struct GcConf;

impl WalConf for GcConf {
  type HeadCache = NoCache;
  type DataCache = SzNoCache;
  type Lock = NoLock;

  fn create(_: &ParsedConf) -> (Self::HeadCache, Self::DataCache, Self::Lock) {
    (NoCache, SzNoCache, NoLock)
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
}

/// Parsed config
/// 解析后的配置
pub struct ParsedConf {
  pub max_size: u64,
  pub cache_size: u64,
  pub file_cap: usize,
  pub write_chan: usize,
}

impl ParsedConf {
  pub fn parse(conf: &[Conf]) -> Self {
    let mut c = Self {
      max_size: DEFAULT_MAX_SIZE,
      cache_size: DEFAULT_CACHE_SIZE,
      file_cap: DEFAULT_FILE_CAP,
      write_chan: DEFAULT_WRITE_CHAN,
    };
    for item in conf {
      match *item {
        Conf::MaxSize(v) => c.max_size = v,
        Conf::CacheSize(v) => c.cache_size = v,
        Conf::FileLru(v) => c.file_cap = v,
        Conf::WriteChan(v) => c.write_chan = v,
      }
    }
    c.file_cap = c.file_cap.max(1);
    c
  }
}
