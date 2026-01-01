//! WAL (Write-Ahead Log) implementation
//! WAL 预写日志实现
//!
//! Single-threaded async with compio
//! 基于 compio 的单线程异步

use jdb_base::{HEAD_SIZE, HEAD_TOTAL, Head, Load, MAGIC};
use zerocopy::FromBytes;

/// WAL entry type for Load trait / WAL 条目类型用于 Load trait
pub struct WalEntry;

impl Load for WalEntry {
  const MAGIC: u8 = MAGIC;
  const HEAD_SIZE: usize = HEAD_TOTAL;
  // Meta is the Head bytes (excludes magic)
  // Meta 是 Head 字节（不含 magic）
  const META_OFFSET: usize = 1;

  #[inline]
  fn len(buf: &[u8]) -> usize {
    if buf.len() < HEAD_TOTAL || buf[0] != MAGIC {
      return 0;
    }
    let Some(head) = Head::read_from_bytes(&buf[1..1 + HEAD_SIZE]).ok() else {
      return 0;
    };
    1 + head.record_size()
  }

  #[inline]
  fn crc_offset(_len: usize) -> usize {
    1 + HEAD_SIZE
  }

  #[inline]
  fn meta_len(_len: usize) -> usize {
    HEAD_SIZE
  }
}

mod conf;
pub(crate) mod consts;
mod header;
mod open;
mod read;
pub(crate) mod record;
mod replay;
mod stream;
mod write;
mod write_buf;

use std::{
  fs,
  marker::PhantomData,
  mem,
  path::PathBuf,
  rc::Rc,
  sync::atomic::{AtomicU64, Ordering},
};

use compio::io::AsyncWriteAtExt;
use compio_fs::File;
use compio_runtime::spawn;
pub use conf::{Conf, DefaultGc, Gc, NoGc, Val};
pub(crate) use conf::{DefaultConf, GcConf, ParsedConf, WalConf};
use consts::{BIN_SUBDIR, WAL_SUBDIR};
use hashlink::lru_cache::Entry;
use ider::Ider;
use jdb_base::{HeadBuilder, Pos};
use jdb_lru::{Cache, Lru};
use size_lru::SizeLru;
use write_buf::SharedState;

use crate::{
  Ckp, Error,
  block_cache::BlockLru,
  error::Result,
  fs::{decode_id, id_path, open_read},
};

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
  read_buf: Vec<u8>,
  block_cache: BlockLru,
  bin_cache: Lru<u64, File>,
  val_cache: C::ValCache,
  head_builder: HeadBuilder,
  /// Checkpoint manager / 检查点管理器
  ckp: Option<Ckp>,
  _marker: PhantomData<C>,
}

// Max single write size (128MB)
// 单次写入最大大小
const MAX_WRITE_SIZE: usize = 128 * 1024 * 1024;

/// Background writer task
/// 后台写入任务
async fn writer_task(shared: Rc<SharedState>) {
  loop {
    let Some(slot) = shared.begin_write() else {
      break;
    };
    let idx = slot.idx;
    let offset = slot.offset;
    let len = slot.len;

    if let Some(f) = shared.file() {
      // Get pointer to data (stays valid until end_write)
      // 获取数据指针（在 end_write 前有效）
      let (ptr, _) = shared.get_write_ptr(idx);

      if len <= MAX_WRITE_SIZE {
        // Small write, single call
        // 小写入，单次调用
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        let _ = f.write_all_at(slice, offset).await;
      } else {
        // Large write, split into chunks
        // 大写入，分批处理
        let mut written = 0;
        while written < len {
          let chunk_size = (len - written).min(MAX_WRITE_SIZE);
          let slice = unsafe { std::slice::from_raw_parts(ptr.add(written), chunk_size) };
          let _ = f.write_all_at(slice, offset + written as u64).await;
          written += chunk_size;
        }
      }
    }

    // Clear slot after write done
    // 写入完成后清空槽
    shared.end_write(idx);
  }
}

/// Check if rotation needed after flush
/// flush 后检查是否需要轮转
#[inline(always)]
fn need_rotate(cur_pos: u64, max_size: u64) -> bool {
  cur_pos >= max_size
}

impl<C: WalConf> WalInner<C> {
  /// Create WAL manager
  /// 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let c = ParsedConf::parse(conf);
    let (val_cache, cur_lock) = C::create(&c);
    let dir = dir.into();
    let wal_dir = dir.join(WAL_SUBDIR);
    Self {
      bin_dir: dir.join(BIN_SUBDIR),
      cur_id: AtomicU64::new(0),
      shared: Rc::new(SharedState::new(c.write_chan, c.slot_max)),
      cur_pos: 0,
      max_size: c.max_size,
      ider: Ider::new(),
      cur_lock,
      read_buf: Vec::new(),
      block_cache: BlockLru::new(&wal_dir, c.file_cap),
      bin_cache: Lru::new(c.bin_cap),
      val_cache,
      head_builder: HeadBuilder::new(),
      ckp: None,
      _marker: PhantomData,
      wal_dir,
    }
  }

  /// Check if pos is in val cache (for debugging)
  /// 检查 pos 是否在 val 缓存中（调试用）
  #[inline]
  pub fn cache_contains(&mut self, pos: &Pos) -> bool {
    self.val_cache.get(pos).is_some()
  }

  #[inline(always)]
  fn maybe_spawn_writer(&self) {
    if !self.shared.is_task_running() {
      self.shared.set_task_running(true);
      spawn(writer_task(Rc::clone(&self.shared))).detach();
    }
  }

  /// Wait if current slot is too large
  /// 如果当前槽太大则等待
  #[inline(always)]
  pub async fn wait_if_full(&self) {
    let slot_max = self.shared.slot_max();
    while self.shared.cur_len() >= slot_max {
      compio_runtime::time::sleep(write_buf::SLEEP_DUR).await;
    }
  }

  pub async fn flush(&mut self) -> Result<()> {
    while !self.shared.is_empty() || self.shared.is_task_running() {
      compio_runtime::time::sleep(write_buf::SLEEP_DUR).await;
    }
    // Check rotation after flush
    // flush 后检查轮转
    if need_rotate(self.cur_pos, self.max_size) {
      self.rotate_inner().await?;
    }
    Ok(())
  }

  #[inline(always)]
  fn wal_path(&self, id: u64) -> PathBuf {
    id_path(&self.wal_dir, id)
  }

  #[inline(always)]
  fn bin_path(&self, id: u64) -> PathBuf {
    id_path(&self.bin_dir, id)
  }

  pub(crate) async fn get_bin_file(&mut self, id: u64) -> Result<&File> {
    let path = self.bin_path(id);
    match self.bin_cache.0.entry(id) {
      Entry::Occupied(e) => Ok(e.into_mut()),
      Entry::Vacant(e) => {
        let file = open_read(&path).await?;
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
        decode_id(name)
      })
  }

  pub async fn rm(&mut self, id: u64) -> Result<()> {
    if id == self.cur_id() {
      return Err(Error::CannotRemoveCurrent);
    }
    let path = self.wal_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.block_cache.rm(id);
    // Note: val_cache entries for this file will be evicted by LHD
    // 注意：该文件的 val_cache 条目将被 LHD 淘汰
    Ok(())
  }

  pub async fn rm_bin(&mut self, id: u64) -> Result<()> {
    let path = self.bin_path(id);
    let _ = compio_fs::remove_file(&path).await;
    self.bin_cache.rm(&id);
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
    self.cur_id.load(Ordering::Relaxed)
  }

  #[inline(always)]
  pub fn cur_pos(&self) -> u64 {
    self.cur_pos
  }

  /// Check if write queue has pending data (for testing)
  /// 检查写入队列是否有待写入数据（测试用）
  #[inline(always)]
  pub fn has_pending(&self) -> bool {
    !self.shared.is_empty() || self.shared.is_task_running()
  }

  /// Get data directory / 获取数据目录
  #[inline(always)]
  pub(crate) fn dir(&self) -> PathBuf {
    self.wal_dir.parent().unwrap().to_path_buf()
  }

  /// Save checkpoint (called by upper layer)
  /// 保存检查点（上层调用）
  pub async fn save_ckp(&mut self) -> Result<()> {
    let id = self.cur_id();
    let offset = self.cur_pos;
    if let Some(ckp) = &mut self.ckp {
      ckp.save(id, offset).await
    } else {
      Err(Error::NotOpen)
    }
  }

  /// Get last save id for GC boundary
  /// 获取最后保存的 id 用于 GC 边界
  #[inline]
  pub fn last_save_id(&self) -> Option<u64> {
    self.ckp.as_ref().and_then(|c| c.last_save_id())
  }
}
