//! Sink discard log
//! 下沉丢弃日志
//!
//! Records discarded Pos during compaction for WAL GC
//! 记录下沉过程中被丢弃的 Pos，用于 WAL GC

use std::{collections::HashMap, io, path::Path};

use compio::io::AsyncWriteAtExt;
use jdb_base::Pos;
use jdb_fs::{
  fs::open_read_write_create,
  fs_id::id_path,
  kv::{self, CRC_SIZE, HEAD_SIZE, to_disk_vec},
};
use jdb_sst::OnDiscard;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Sink directory name
/// 下沉目录名
pub const SINK_DIR: &str = "sink";

/// Count file name
/// 统计文件名
const COUNT_FILE: &str = "sink.count";

/// Pos entry kind
/// Pos 条目类型
const KIND_POS: u8 = 2;

/// Pos disk size
/// Pos 磁盘大小
const POS_DISK_SIZE: usize = HEAD_SIZE + size_of::<Pos>() + CRC_SIZE;

/// Count entry (wal_id → count)
/// 统计条目（wal_id → count）
#[repr(C)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
struct CountEntry {
  wal_id: u64,
  count: u64,
}

impl kv::Entry for CountEntry {
  type Key = u64;
  type Val = u64;

  const KIND: u8 = 1;

  #[inline]
  fn new(key: u64, val: u64) -> Self {
    Self {
      wal_id: key,
      count: val,
    }
  }

  #[inline]
  fn key(&self) -> u64 {
    self.wal_id
  }

  #[inline]
  fn val(&self) -> u64 {
    self.count
  }

  #[inline]
  fn is_remove(&self) -> bool {
    self.count == 0
  }
}

/// Sink log - collects discarded Pos grouped by wal_id
/// 下沉日志 - 按 wal_id 分组收集被丢弃的 Pos
#[derive(Default)]
pub struct SinkLog {
  by_wal: HashMap<u64, Vec<Pos>>,
}

impl SinkLog {
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.by_wal.is_empty()
  }

  /// Total discarded count
  /// 总丢弃数量
  #[inline]
  pub fn count(&self) -> usize {
    self.by_wal.values().map(|v| v.len()).sum()
  }

  /// Iterate by wal_id
  /// 按 wal_id 迭代
  #[inline]
  pub fn iter(&self) -> impl Iterator<Item = (&u64, &Vec<Pos>)> {
    self.by_wal.iter()
  }
}

impl OnDiscard for SinkLog {
  #[inline]
  fn discard(&mut self, _key: &[u8], pos: &Pos) {
    self.by_wal.entry(pos.wal_id()).or_default().push(*pos);
  }
}

/// Sink count manager - in-memory stats with persistent storage
/// 下沉统计管理器 - 内存统计 + 持久化存储
pub struct SinkCount {
  counts: HashMap<u64, u64>,
  path: std::path::PathBuf,
}

impl SinkCount {
  /// Load from file, rewrite on startup
  /// 从文件加载，启动时重写
  pub async fn load(dir: &Path) -> io::Result<Self> {
    let sink_dir = dir.join(SINK_DIR);
    compio::fs::create_dir_all(&sink_dir).await?;

    let path = sink_dir.join(COUNT_FILE);
    let counts = kv::load::<CountEntry>(&path).await?;

    let mut this = Self { counts, path };
    // Rewrite on startup to compact
    // 启动时重写以压缩
    this.rewrite().await?;
    Ok(this)
  }

  /// Get count for wal_id
  /// 获取 wal_id 的计数
  #[inline]
  pub fn get(&self, wal_id: u64) -> u64 {
    self.counts.get(&wal_id).copied().unwrap_or(0)
  }

  /// Get all counts
  /// 获取所有计数
  #[inline]
  pub fn all(&self) -> &HashMap<u64, u64> {
    &self.counts
  }

  /// Add counts from SinkLog
  /// 从 SinkLog 添加计数
  pub fn add(&mut self, log: &SinkLog) {
    for (&wal_id, positions) in log.iter() {
      *self.counts.entry(wal_id).or_default() += positions.len() as u64;
    }
  }

  /// Remove wal_id (after WAL GC)
  /// 移除 wal_id（WAL GC 后）
  #[inline]
  pub fn remove(&mut self, wal_id: u64) {
    self.counts.remove(&wal_id);
  }

  /// Rewrite count file (atomic)
  /// 重写统计文件（原子）
  pub async fn rewrite(&mut self) -> io::Result<()> {
    kv::rewrite::<CountEntry>(&self.path, &self.counts).await
  }
}

/// Flush discarded positions to sink/{wal_id} files
/// 将丢弃的位置刷盘到 sink/{wal_id} 文件
pub async fn flush_positions(dir: &Path, log: &SinkLog) -> io::Result<()> {
  if log.is_empty() {
    return Ok(());
  }

  let sink_dir = dir.join(SINK_DIR);
  compio::fs::create_dir_all(&sink_dir).await?;

  for (&wal_id, positions) in log.iter() {
    if positions.is_empty() {
      continue;
    }

    let path = id_path(&sink_dir, wal_id);
    let mut file = open_read_write_create(&path).await?;

    // Append positions with CRC
    // 追加带 CRC 的位置
    let meta = file.metadata().await?;
    let offset = meta.len();

    let mut buf = Vec::with_capacity(positions.len() * POS_DISK_SIZE);
    for pos in positions {
      buf.extend_from_slice(&to_disk_vec(KIND_POS, pos));
    }

    file.write_all_at(buf, offset).await.0?;
    file.sync_all().await?;
  }

  Ok(())
}
