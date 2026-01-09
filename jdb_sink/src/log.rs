//! Sink count manager
//! 下沉统计管理器

use std::{collections::HashMap, io, path::Path};

use jdb_fs::kv;
use jdb_gc::{GC_DIR, GcLog};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Re-export GcLog as SinkLog for backward compatibility
/// 重新导出 GcLog 为 SinkLog 以保持向后兼容
pub type SinkLog = GcLog;

/// Count file name
/// 统计文件名
const COUNT_FILE: &str = "sink.count";

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
    let sink_dir = dir.join(GC_DIR);
    compio::fs::create_dir_all(&sink_dir).await?;

    let path = sink_dir.join(COUNT_FILE);
    let counts = kv::load::<CountEntry>(&path).await?;

    let mut this = Self { counts, path };
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

  /// Add counts from GcLog
  /// 从 GcLog 添加计数
  pub fn add(&mut self, log: &GcLog) {
    log.for_each(|wal_id, positions| {
      *self.counts.entry(wal_id).or_default() += positions.len() as u64;
    });
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
