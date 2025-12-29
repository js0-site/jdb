//! WAL (Write-Ahead Log) implementation
//! WAL 预写日志实现
//!
//! Single-threaded async with compio
//! 基于 compio 的单线程异步

mod conf;
pub(crate) mod consts;
mod header;
/// LZ4 compression module (public for testing)
/// LZ4 压缩模块（公开用于测试）
pub mod lz4;
mod open;
mod read;
mod stream;
mod write;

use std::{
  cell::Cell,
  fs,
  marker::PhantomData,
  mem,
  path::PathBuf,
  rc::Rc,
  sync::atomic::{AtomicU64, Ordering},
};

use compio::io::AsyncWriteAtExt;
use compio_fs::{File, OpenOptions};
use compio_runtime::spawn;
pub use conf::{CachedData, Conf, DefaultConf, Gc, GcConf, ParsedConf, WalConf};
use consts::{BIN_SUBDIR, WAL_SUBDIR};
pub use consts::{HEADER_SIZE, WAL_VERSION};
use fast32::base32::CROCKFORD_LOWER;
use hashlink::lru_cache::Entry;
use ider::Ider;
pub use jdb_lru::{Cache, Lru, NoCache};
pub use read::LogIter;
pub use stream::DataStream;

use crate::{Error, HeadBuilder, INFILE_MAX, error::Result};

/// Max record size for queue slot
/// 队列槽位最大记录大小
const MAX_SLOT_SIZE: usize = 2 * INFILE_MAX + 256;

/// Average entry size estimate for buffer allocation
/// 用于缓冲分配的平均条目大小估计
const AVG_ENTRY_SIZE: usize = 128;

/// Shared write state
/// 共享写入状态
struct WriteState {
  buf: Vec<u8>,
  start_pos: u64,
  spare: Option<Vec<u8>>,
  writing: bool,
}

/// Shared state wrapper
/// 共享状态包装器
struct SharedState {
  state: Cell<WriteState>,
  file: Cell<Option<File>>,
}

impl SharedState {
  fn new(cap: usize) -> Self {
    Self {
      state: Cell::new(WriteState {
        buf: Vec::with_capacity(cap * AVG_ENTRY_SIZE),
        start_pos: 0,
        spare: None,
        writing: false,
      }),
      file: Cell::new(None),
    }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)]
  fn state(&self) -> &mut WriteState {
    unsafe { &mut *self.state.as_ptr() }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)]
  fn file(&self) -> &mut Option<File> {
    unsafe { &mut *self.file.as_ptr() }
  }

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

  #[inline(always)]
  fn is_empty(&self) -> bool {
    self.state().buf.is_empty()
  }

  #[inline(always)]
  fn is_writing(&self) -> bool {
    self.state().writing
  }

  #[inline(always)]
  fn set_writing(&self, v: bool) {
    self.state().writing = v;
  }

  #[inline(always)]
  fn take(&self) -> (Vec<u8>, u64) {
    let s = self.state();
    let new_buf = s.spare.take().unwrap_or_default();
    let buf = mem::replace(&mut s.buf, new_buf);
    (buf, s.start_pos)
  }

  #[inline(always)]
  fn recycle(&self, mut buf: Vec<u8>) {
    buf.clear();
    if buf.capacity() > MAX_SLOT_SIZE {
      buf.shrink_to(MAX_SLOT_SIZE);
    }
    let s = self.state();
    if s.spare.is_none() {
      s.spare = Some(buf);
    }
  }

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

/// WAL manager with LRU cache
/// 带 LRU 缓存的 WAL 管理器
pub type Wal = WalInner<DefaultConf>;

/// WAL manager without cache (for GC)
/// 无缓存的 WAL 管理器（用于 GC）
pub type WalNoCache = WalInner<GcConf>;

/// WAL manager
/// WAL 管理器
pub struct WalInner<C: WalConf> {
  wal_dir: PathBuf,
  bin_dir: PathBuf,
  /// Current WAL id (atomic for GC thread safety)
  /// 当前 WAL id（原子操作保证 GC 线程安全）
  cur_id: AtomicU64,
  shared: Rc<SharedState>,
  cur_pos: u64,
  max_size: u64,
  /// ID generator (public for GC)
  /// ID 生成器（公开用于 GC）
  pub ider: Ider,
  cur_lock: C::Lock,
  data_buf: Vec<u8>,
  read_buf: Vec<u8>,
  file_cache: Lru<u64, File>,
  val_cache: C::ValCache,
  head_builder: HeadBuilder,
  _marker: PhantomData<C>,
}

/// Background writer task
/// 后台写入任务
async fn writer_task(shared: Rc<SharedState>) {
  loop {
    let (buf, start_pos) = shared.take();
    if buf.is_empty() {
      shared.set_writing(false);
      break;
    }
    if let Some(f) = shared.file() {
      let res = f.write_all_at(buf, start_pos).await;
      shared.recycle(res.1);
    }
  }
}

impl<C: WalConf> WalInner<C> {
  /// Create WAL manager
  /// 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let c = ParsedConf::parse(conf);
    let (val_cache, cur_lock) = C::create(&c);
    let dir = dir.into();
    Self {
      wal_dir: dir.join(WAL_SUBDIR),
      bin_dir: dir.join(BIN_SUBDIR),
      cur_id: AtomicU64::new(0),
      shared: Rc::new(SharedState::new(c.write_chan)),
      cur_pos: 0,
      max_size: c.max_size,
      ider: Ider::new(),
      cur_lock,
      data_buf: Vec::new(),
      read_buf: Vec::new(),
      file_cache: Lru::new(c.file_cap),
      val_cache,
      head_builder: HeadBuilder::new(),
      _marker: PhantomData,
    }
  }

  #[inline(always)]
  fn maybe_spawn_writer(&self) {
    if !self.shared.is_writing() {
      self.shared.set_writing(true);
      spawn(writer_task(Rc::clone(&self.shared))).detach();
    }
  }

  pub async fn flush(&mut self) -> Result<()> {
    while !self.shared.is_empty() || self.shared.is_writing() {
      compio_runtime::time::sleep(std::time::Duration::from_micros(1)).await;
    }
    Ok(())
  }

  #[inline(always)]
  pub(crate) fn encode_id(id: u64) -> String {
    CROCKFORD_LOWER.encode_u64(id)
  }

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

  #[inline(always)]
  #[allow(clippy::uninit_vec)]
  pub(crate) fn prepare_buf(buf_slot: &mut Vec<u8>, len: usize) -> Vec<u8> {
    let mut buf = mem::take(buf_slot);
    buf.clear();
    buf.reserve(len);
    unsafe { buf.set_len(len) };
    buf
  }

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

  pub async fn rm(&mut self, id: u64) -> Result<()> {
    if id == self.cur_id() {
      return Err(Error::CannotRemoveCurrent);
    }
    let path = self.wal_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.file_cache.rm(&id);
    // Note: val_cache entries for this file will be evicted by LHD
    // 注意：该文件的 val_cache 条目将被 LHD 淘汰
    Ok(())
  }

  pub async fn rm_bin(&mut self, id: u64) -> Result<()> {
    let path = self.bin_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.file_cache.rm(&id);
    Ok(())
  }

  pub async fn sync_data(&mut self) -> Result<()> {
    self.flush().await?;
    if let Some(file) = self.shared.file() {
      file.sync_data().await?;
    }
    Ok(())
  }

  pub async fn sync_all(&mut self) -> Result<()> {
    self.flush().await?;
    if let Some(file) = self.shared.file() {
      file.sync_all().await?;
    }
    Ok(())
  }

  #[inline(always)]
  pub fn cur_id(&self) -> u64 {
    self.cur_id.load(Ordering::Acquire)
  }

  #[inline(always)]
  pub fn cur_pos(&self) -> u64 {
    self.cur_pos
  }
}
