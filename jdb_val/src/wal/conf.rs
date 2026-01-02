//! WAL configuration
//! WAL 配置

use std::rc::Rc;

use jdb_base::{Flag, Pos};
use jdb_lock::{NoLock, WalLock, w::Lock as WLock};
use size_lru::{Lhd, NoCache as SzNoCache, SizeLru};

use super::consts::{
  BLOCK_CACHE_RATIO, DEFAULT_BIN_CAP, DEFAULT_BUF_MAX, DEFAULT_CACHE_SIZE, DEFAULT_FILE_CAP,
  DEFAULT_MAX_SIZE, DEFAULT_WRITE_CHAN, MIN_BIN_CAP, MIN_CACHE_SIZE, MIN_FILE_CAP,
};

/// GC trait for data processing during GC
/// GC 数据处理 trait
pub trait Gc: Default {
  /// Process data, may compress
  /// 处理数据，可能压缩
  ///
  /// Returns (new_flag, compressed_len)
  /// 返回 (新标志, 压缩后长度)
  fn process(&mut self, flag: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>);
}

/// Default GC (no compression, upstream handles it)
/// 默认 GC（不压缩，上游处理）
#[derive(Default)]
pub struct DefaultGc;

impl Gc for DefaultGc {
  fn process(&mut self, flag: Flag, _data: &[u8], _buf: &mut Vec<u8>) -> (Flag, Option<usize>) {
    (flag, None)
  }
}

/// No-op GC (no compression)
/// 无操作 GC（不压缩）
#[derive(Default)]
pub struct NoGc;

impl Gc for NoGc {
  fn process(&mut self, flag: Flag, _data: &[u8], _buf: &mut Vec<u8>) -> (Flag, Option<usize>) {
    (flag, None)
  }
}

/// Cached value type
/// 缓存值类型
pub type Val = Rc<[u8]>;

/// WAL configuration trait
/// WAL 配置 trait
pub trait WalConf {
  type ValCache: SizeLru<Pos, Val>;
  type Lock: WalLock;
  type Gc: Gc;

  /// Create cache and lock
  /// 创建缓存和锁
  fn create(conf: &ParsedConf) -> (Self::ValCache, Self::Lock);
}

/// Default WAL config (with LHD cache, lock, and LZ4 GC)
/// 默认 WAL 配置（带 LHD 缓存、锁和 LZ4 GC）
pub struct DefaultConf;

impl WalConf for DefaultConf {
  type ValCache = Lhd<Pos, Val>;
  type Lock = WLock;
  type Gc = DefaultGc;

  fn create(conf: &ParsedConf) -> (Self::ValCache, Self::Lock) {
    (Lhd::new(conf.cache_size as usize), WLock::default())
  }
}

/// GC WAL config (no cache, no lock, no compression)
/// GC WAL 配置（无缓存、无锁、无压缩）
pub struct GcConf;

impl WalConf for GcConf {
  type ValCache = SzNoCache;
  type Lock = NoLock;
  type Gc = NoGc;

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
  pub buf_max: usize,
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
      buf_max: DEFAULT_BUF_MAX,
    };
    for item in conf {
      match *item {
        Conf::MaxSize(v) => c.max_size = v,
        Conf::CacheSize(v) => c.cache_size = v,
        Conf::FileLru(v) => c.file_cap = v,
        Conf::WriteChan(v) => c.write_chan = v,
        Conf::SlotMax(v) => c.buf_max = v,
      }
    }
    c.cache_size = c.cache_size.max(MIN_CACHE_SIZE);
    c.block_cache_size = c.cache_size * BLOCK_CACHE_RATIO / 100;
    c.file_cap = c.file_cap.max(MIN_FILE_CAP);
    c.bin_cap = c.bin_cap.max(MIN_BIN_CAP);
    c
  }
}
