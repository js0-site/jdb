//! WAL (Write-Ahead Log) implementation / WAL 预写日志实现
//!
//! Single-threaded async with compio / 基于 compio 的单线程异步

pub(crate) mod consts;
mod header;
mod open;
mod read;
mod stream;
mod write;

use std::{cell::Cell, fs, mem, path::PathBuf, rc::Rc};

use compio::io::AsyncWriteAtExt;
use compio_fs::{File, OpenOptions};
use compio_runtime::spawn;
use consts::{
  BIN_SUBDIR, DEFAULT_CACHE_SIZE, DEFAULT_FILE_CAP, DEFAULT_MAX_SIZE, DEFAULT_WRITE_CHAN,
  WAL_SUBDIR, calc_cache_cap,
};
pub use consts::{HEADER_SIZE, MAGIC, MAGIC_SIZE, WAL_VERSION};
use fast32::base32::CROCKFORD_LOWER;
use hashlink::lru_cache::Entry;
use ider::Ider;
pub use jdb_lru::{Cache, Lru, NoCache};
pub use read::LogIter;
pub use stream::DataStream;

use crate::{Error, Head, INFILE_MAX, Pos, error::Result};

/// Cached data type / 缓存数据类型
pub type CachedData = Rc<[u8]>;

/// Max record size for queue slot / 队列槽位最大记录大小
/// Head(64) + max_infile_key(1MB) + max_infile_val(1MB)
const MAX_SLOT_SIZE: usize = Head::SIZE + 2 * INFILE_MAX + 128;

/// Average entry size estimate for buffer allocation / 用于缓冲分配的平均条目大小估计
/// Optimized based on typical WAL usage patterns / 基于典型 WAL 使用模式优化
const AVG_ENTRY_SIZE: usize = 128;

/// Shared write state / 共享写入状态
struct WriteState {
  /// Data buffer / 数据缓冲区
  buf: Vec<u8>,
  /// Start position of first write / 第一次写入的起始位置
  start_pos: u64,
  /// Spare buffer for recycling / 用于回收的备用缓冲区
  spare: Option<Vec<u8>>,
  /// Writer task running / 写入任务运行中
  writing: bool,
}

/// Shared state wrapper / 共享状态包装器
struct SharedState {
  state: Cell<WriteState>,
  file: Cell<Option<File>>,
}

impl SharedState {
  fn new(cap: usize) -> Self {
    Self {
      state: Cell::new(WriteState {
        // Optimize memory: use average size instead of worst-case
        // 内存优化：使用平均大小而非最坏情况
        buf: Vec::with_capacity(cap * AVG_ENTRY_SIZE),
        start_pos: 0,
        spare: None,
        writing: false,
      }),
      file: Cell::new(None),
    }
  }

  /// SAFETY: single-threaded async / 安全：单线程异步
  #[inline(always)]
  #[allow(clippy::mut_from_ref)] // Safe: single-threaded compio runtime / 安全：单线程 compio 运行时
  fn state(&self) -> &mut WriteState {
    unsafe { &mut *self.state.as_ptr() }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)] // Safe: single-threaded compio runtime / 安全：单线程 compio 运行时
  fn file(&self) -> &mut Option<File> {
    unsafe { &mut *self.file.as_ptr() }
  }

  /// Push multiple slices directly to queue / 直接推入多个切片到队列
  #[inline(always)]
  fn push_slices(&self, pos: u64, parts: &[&[u8]], len: usize) {
    let s = self.state();
    if s.buf.is_empty() {
      s.start_pos = pos;
    }
    s.buf.reserve(len);
    for p in parts {
      s.buf.extend_from_slice(p);
    }
  }

  /// Check if queue is empty / 检查队列是否为空
  #[inline(always)]
  fn is_empty(&self) -> bool {
    self.state().buf.is_empty()
  }

  /// Check if writer is running / 检查写入任务是否运行中
  #[inline(always)]
  fn is_writing(&self) -> bool {
    self.state().writing
  }

  /// Set writing flag / 设置写入标志
  #[inline(always)]
  fn set_writing(&self, v: bool) {
    self.state().writing = v;
  }

  /// Take pending writes / 取出待写入数据
  #[inline(always)]
  fn take(&self) -> (Vec<u8>, u64) {
    let s = self.state();
    // Reuse spare buffer if available / 如果可用，复用备用缓冲区
    let new_buf = s.spare.take().unwrap_or_default();
    let buf = mem::replace(&mut s.buf, new_buf);
    (buf, s.start_pos)
  }

  /// Recycle buffer from writer / 回收写入器的缓冲区
  #[inline(always)]
  fn recycle(&self, mut buf: Vec<u8>) {
    buf.clear();
    // Memory safeguard: shrink buffer if it grew too large (e.g. > 1MB) during a spike
    // 内存保护：如果缓冲区因突发流量变得过大（如 > 1MB），则收缩
    if buf.capacity() > INFILE_MAX {
      buf.shrink_to(INFILE_MAX);
    }

    let s = self.state();
    // Keep one spare buffer / 保留一个备用缓冲区
    if s.spare.is_none() {
      s.spare = Some(buf);
    }
  }

  /// Find data by file pos / 按文件位置查找数据
  #[inline(always)]
  fn find_by_pos(&self, file_pos: u64, need_len: usize) -> Option<&[u8]> {
    let s = self.state();
    if s.buf.is_empty() {
      return None;
    }
    let start = s.start_pos;
    let end = start + s.buf.len() as u64;
    if file_pos >= start && file_pos + need_len as u64 <= end {
      let off = (file_pos - start) as usize;
      return Some(unsafe { s.buf.get_unchecked(off..off + need_len) });
    }
    None
  }
}

/// Storage mode / 存储模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
  Inline,
  Infile,
  File,
}

/// WAL manager with LRU cache / 带 LRU 缓存的 WAL 管理器
pub type Wal = WalInner<Lru<Pos, Head>, Lru<Pos, CachedData>>;

/// WAL manager without cache (for GC) / 无缓存的 WAL 管理器（用于 GC）
pub type WalNoCache = WalInner<NoCache, NoCache>;

/// WAL manager / WAL 管理器
pub struct WalInner<HC: Cache<Pos, Head>, DC: Cache<Pos, CachedData>> {
  wal_dir: PathBuf,
  bin_dir: PathBuf,
  cur_id: u64,
  shared: Rc<SharedState>,
  cur_pos: u64,
  max_size: u64,
  ider: Ider,
  /// Data buffer for writes / 写入数据缓冲区
  data_buf: Vec<u8>,
  /// Buffer for reading data / 读取数据缓冲区
  read_buf: Vec<u8>,
  /// Head cache / Head 缓存
  head_cache: HC,
  /// File handle cache / 文件句柄缓存
  file_cache: Lru<u64, File>,
  /// Infile data cache (File mode not cached) / Infile 数据缓存（File 模式不缓存）
  data_cache: DC,
}

/// Background writer task / 后台写入任务
async fn writer_task(shared: Rc<SharedState>) {
  loop {
    let (buf, start_pos) = shared.take();
    if buf.is_empty() {
      shared.set_writing(false);
      break;
    }

    if let Some(f) = shared.file() {
      // Write and recycle buffer / 写入并回收缓冲区
      let res = f.write_all_at(buf, start_pos).await;
      shared.recycle(res.1);
    }
  }
}

/// WAL configuration / WAL 配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Max WAL file size in bytes / 最大文件大小（字节）
  MaxSize(u64),
  /// Total cache size in bytes (auto-split to head/data caches)
  /// 总缓存大小（字节），自动分配到 head/data 缓存
  CacheSize(u64),
  /// File handle cache capacity / 文件句柄缓存容量
  FileLru(usize),
  /// Write queue capacity / 写入队列容量
  WriteChan(usize),
}

/// Parsed config / 解析后的配置
struct ParsedConf {
  max_size: u64,
  cache_size: u64,
  file_cap: usize,
  write_chan: usize,
}

impl ParsedConf {
  fn parse(conf: &[Conf]) -> Self {
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

impl Wal {
  /// Create WAL manager / 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let c = ParsedConf::parse(conf);
    let (head_cap, data_cap) = calc_cache_cap(c.cache_size);
    WalInner::init(dir, &c, Lru::new(head_cap), Lru::new(data_cap))
  }
}

impl WalNoCache {
  /// Create WAL manager without cache (for GC) / 创建无缓存的 WAL 管理器（用于 GC）
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let c = ParsedConf::parse(conf);
    WalInner::init(dir, &c, NoCache, NoCache)
  }
}

impl<HC: Cache<Pos, Head>, DC: Cache<Pos, CachedData>> WalInner<HC, DC> {
  /// Init WAL inner / 初始化 WAL 内部
  fn init(dir: impl Into<PathBuf>, c: &ParsedConf, head_cache: HC, data_cache: DC) -> Self {
    let dir = dir.into();
    Self {
      wal_dir: dir.join(WAL_SUBDIR),
      bin_dir: dir.join(BIN_SUBDIR),
      cur_id: 0,
      shared: Rc::new(SharedState::new(c.write_chan)),
      cur_pos: 0,
      max_size: c.max_size,
      ider: Ider::new(),
      data_buf: Vec::new(),
      read_buf: Vec::new(),
      head_cache,
      file_cache: Lru::new(c.file_cap),
      data_cache,
    }
  }

  /// Spawn writer if not running / 如果未运行则启动写入任务
  #[inline(always)]
  fn maybe_spawn_writer(&self) {
    if !self.shared.is_writing() {
      self.shared.set_writing(true);
      spawn(writer_task(Rc::clone(&self.shared))).detach();
    }
  }

  /// Flush pending writes / 刷新待写入数据
  pub async fn flush(&mut self) -> Result<()> {
    while !self.shared.is_empty() || self.shared.is_writing() {
      compio_runtime::time::sleep(std::time::Duration::from_micros(1)).await;
    }
    Ok(())
  }

  /// Helper to encode ID to filename / ID 编码为文件名的辅助函数
  #[inline(always)]
  pub(crate) fn encode_id(id: u64) -> String {
    CROCKFORD_LOWER.encode_u64(id)
  }

  /// Helper to decode ID from filename / 文件名解码为 ID 的辅助函数
  #[inline(always)]
  pub(crate) fn decode_id(name: &str) -> Option<u64> {
    CROCKFORD_LOWER.decode_u64(name.as_bytes()).ok()
  }

  #[inline(always)]
  fn wal_path(&self, id: u64) -> PathBuf {
    self.wal_dir.join(Self::encode_id(id))
  }

  #[inline(always)]
  fn bin_path(&self, id: u64) -> PathBuf {
    self.bin_dir.join(Self::encode_id(id))
  }

  /// Get cached file or open new one / 获取缓存文件或打开新文件
  pub(crate) async fn get_cached_file(&mut self, id: u64, is_wal: bool) -> Result<&File> {
    let path = if is_wal {
      self.wal_path(id)
    } else {
      self.bin_path(id)
    };
    match self.file_cache.0.entry(id) {
      Entry::Occupied(e) => Ok(e.into_mut()),
      Entry::Vacant(e) => {
        let file = OpenOptions::new().read(true).open(&path).await?;
        Ok(e.insert(file))
      }
    }
  }

  /// Helper to take a buffer and ensure it has exact capacity/length
  /// 辅助函数：取出缓冲区并确保其具有准确的容量/长度
  #[inline(always)]
  #[allow(clippy::uninit_vec)]
  pub(crate) fn prepare_buf(buf_slot: &mut Vec<u8>, len: usize) -> Vec<u8> {
    let mut buf = mem::take(buf_slot);
    buf.clear();
    buf.reserve(len);
    unsafe { buf.set_len(len) };
    buf
  }

  /// Iterate all WAL file ids / 迭代所有 WAL 文件 id
  pub fn iter(&self) -> impl Iterator<Item = u64> {
    let entries = fs::read_dir(&self.wal_dir).ok();
    entries
      .into_iter()
      .flatten()
      .filter_map(|e| e.ok())
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        Self::decode_id(name)
      })
  }

  /// Remove WAL file (for GC) / 删除 WAL 文件（用于垃圾回收）
  pub async fn rm(&mut self, id: u64) -> Result<()> {
    if id == self.cur_id {
      return Err(Error::CannotRemoveCurrent);
    }
    let path = self.wal_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.file_cache.rm(&id);
    Ok(())
  }

  /// Remove bin file / 删除二进制文件
  pub async fn rm_bin(&mut self, id: u64) -> Result<()> {
    let path = self.bin_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.file_cache.rm(&id);
    Ok(())
  }

  /// Sync data to disk / 同步数据到磁盘
  pub async fn sync_data(&mut self) -> Result<()> {
    self.flush().await?;
    if let Some(file) = self.shared.file() {
      file.sync_data().await?;
    }
    Ok(())
  }

  /// Sync all to disk / 同步所有到磁盘
  pub async fn sync_all(&mut self) -> Result<()> {
    self.flush().await?;
    if let Some(file) = self.shared.file() {
      file.sync_all().await?;
    }
    Ok(())
  }

  #[inline(always)]
  pub fn cur_id(&self) -> u64 {
    self.cur_id
  }

  #[inline(always)]
  pub fn cur_pos(&self) -> u64 {
    self.cur_pos
  }
}
