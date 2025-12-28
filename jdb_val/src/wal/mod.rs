//! WAL (Write-Ahead Log) implementation / WAL 预写日志实现
//!
//! Single-threaded async with compio / 基于 compio 的单线程异步

mod consts;
mod end;
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
  BIN_SUBDIR, DEFAULT_DATA_CAP, DEFAULT_FILE_CAP, DEFAULT_HEAD_CAP, DEFAULT_MAX_SIZE,
  DEFAULT_WRITE_CHAN, WAL_SUBDIR,
};
pub use consts::{END_MAGIC, END_SIZE, HEADER_SIZE, WAL_VERSION};
pub use end::{build_end, parse_end};
use fast32::base32::CROCKFORD_LOWER;
use hashlink::{LruCache, lru_cache::Entry};
use ider::Ider;
pub use read::LogIter;
pub use stream::DataStream;

use crate::{Error, Head, Pos, error::Result};

/// Cached data type / 缓存数据类型
pub type CachedData = Rc<[u8]>;

/// Max record size for queue slot / 队列槽位最大记录大小
/// Head(64) + max_infile_key(64KB) + max_infile_val(64KB) + End(12)
const SLOT_SIZE: usize = Head::SIZE + END_SIZE + 128 * 1024 + 128;

/// Shared write state / 共享写入状态
struct WriteState {
  /// Data buffer / 数据缓冲区
  buf: Vec<u8>,
  /// Write positions: (pos, len) / 写入位置：(位置, 长度)
  reqs: Vec<(u64, usize)>,
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
        buf: Vec::with_capacity(cap * SLOT_SIZE),
        reqs: Vec::with_capacity(cap),
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

  /// Push data to queue / 推入数据到队列
  #[inline(always)]
  fn push(&self, pos: u64, data: &[u8]) {
    let s = self.state();
    s.reqs.push((pos, data.len()));
    s.buf.extend_from_slice(data);
  }

  /// Check if queue is empty / 检查队列是否为空
  #[inline(always)]
  fn is_empty(&self) -> bool {
    self.state().reqs.is_empty()
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
  fn take(&self) -> (Vec<u8>, Vec<(u64, usize)>) {
    let s = self.state();
    (mem::take(&mut s.buf), mem::take(&mut s.reqs))
  }

  /// Find data by file pos / 按文件位置查找数据
  #[inline(always)]
  fn find_by_pos(&self, file_pos: u64, need_len: usize) -> Option<&[u8]> {
    let s = self.state();
    let mut off = 0usize;
    for &(pos, len) in &s.reqs {
      if pos <= file_pos && file_pos + need_len as u64 <= pos + len as u64 {
        let start = off + (file_pos - pos) as usize;
        return Some(unsafe { s.buf.get_unchecked(start..start + need_len) });
      }
      off += len;
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

/// WAL manager / WAL 管理器
pub struct Wal {
  wal_dir: PathBuf,
  bin_dir: PathBuf,
  cur_id: u64,
  shared: Rc<SharedState>,
  cur_pos: u64,
  max_size: u64,
  ider: Ider,
  /// Scratch buffer for writing headers/metadata to avoid repeated allocations
  /// 用于写入头/元数据的临时缓冲区，避免重复分配
  scratch: Vec<u8>,
  /// Data buffer for infile writes / Infile 写入的数据缓冲区
  data_buf: Vec<u8>,
  /// Buffer for file writes to avoid allocation / 文件写入缓冲区，避免分配
  file_buf: Vec<u8>,
  /// Buffer for reading data to avoid allocation / 读取数据缓冲区，避免分配
  read_buf: Vec<u8>,
  head_cache: LruCache<Pos, Head>,
  file_cache: LruCache<u64, File>,
  data_cache: LruCache<Pos, CachedData>,
}

/// Background writer task / 后台写入任务
async fn writer_task(shared: Rc<SharedState>) {
  loop {
    let (buf, reqs) = shared.take();
    if reqs.is_empty() {
      shared.set_writing(false);
      break;
    }

    if let Some(f) = shared.file() {
      let mut off = 0usize;
      for (pos, len) in reqs {
        let data = buf[off..off + len].to_vec();
        let _ = f.write_all_at(data, pos).await;
        off += len;
      }
    }
  }
}

/// WAL configuration / WAL 配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Max WAL file size in bytes / 最大文件大小（字节）
  MaxSize(u64),
  /// Head cache capacity / 头缓存容量
  HeadLru(usize),
  /// Data cache capacity / 数据缓存容量
  DataLru(usize),
  /// File handle cache capacity / 文件句柄缓存容量
  FileLru(usize),
  /// Write channel capacity / 写入通道容量
  WriteChan(usize),
}

impl Wal {
  /// Create WAL manager / 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let dir = dir.into();
    let mut max_size = DEFAULT_MAX_SIZE;
    let mut head_cap = DEFAULT_HEAD_CAP;
    let mut data_cap = DEFAULT_DATA_CAP;
    let mut file_cap = DEFAULT_FILE_CAP;
    let mut write_chan_cap = DEFAULT_WRITE_CHAN;

    for c in conf {
      match *c {
        Conf::MaxSize(v) => max_size = v,
        Conf::HeadLru(v) => head_cap = v,
        Conf::DataLru(v) => data_cap = v,
        Conf::FileLru(v) => file_cap = v,
        Conf::WriteChan(v) => write_chan_cap = v,
      }
    }

    Self {
      wal_dir: dir.join(WAL_SUBDIR),
      bin_dir: dir.join(BIN_SUBDIR),
      cur_id: 0,
      shared: Rc::new(SharedState::new(write_chan_cap)),
      cur_pos: 0,
      max_size,
      ider: Ider::new(),
      scratch: Vec::with_capacity(Head::SIZE + END_SIZE),
      data_buf: Vec::new(),
      file_buf: Vec::new(),
      read_buf: Vec::new(),
      head_cache: LruCache::new(head_cap),
      file_cache: LruCache::new(file_cap),
      data_cache: LruCache::new(data_cap),
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
    match self.file_cache.entry(id) {
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
  pub async fn remove(&mut self, id: u64) -> Result<()> {
    if id == self.cur_id {
      return Err(Error::CannotRemoveCurrent);
    }
    let path = self.wal_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.file_cache.remove(&id);
    Ok(())
  }

  /// Remove bin file / 删除二进制文件
  pub async fn remove_bin(&mut self, id: u64) -> Result<()> {
    let path = self.bin_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.file_cache.remove(&id);
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
