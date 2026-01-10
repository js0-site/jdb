//! GC count - WAL discard size statistics
//! GC 统计 - WAL 丢弃大小统计

use std::{collections::HashMap, io, path::Path};

use jdb_base::WalId;
use jdb_fs::{
  compact::{AutoCompact, Compact},
  kv::{Entry, to_disk_vec},
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Count file name
/// 统计文件名
const COUNT_FILE: &str = "gc/wal.count";

/// Count entry kind
/// 统计条目类型
const KIND: u8 = 3;

/// Compact threshold
/// 压缩阈值
const COMPACT_THRESHOLD: usize = 1024;

/// Size type
/// 大小类型
pub type Size = u64;

/// Count entry (wal_id → size)
/// 统计条目（wal_id → size）
#[repr(C)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct CountEntry {
  pub wal_id: WalId,
  pub size: Size,
}

impl Entry for CountEntry {
  type Key = WalId;
  type Val = Size;

  const KIND: u8 = KIND;

  #[inline]
  fn new(wal_id: WalId, size: Size) -> Self {
    Self { wal_id, size }
  }

  #[inline]
  fn key(&self) -> WalId {
    self.wal_id
  }

  #[inline]
  fn val(&self) -> Size {
    self.size
  }

  #[inline]
  fn is_remove(&self) -> bool {
    self.size == 0
  }
}

/// GC count inner - total size statistics
/// GC 统计内部 - 全量大小统计
#[derive(Default)]
pub struct CountInner {
  /// Total sizes (from disk)
  /// 全量大小（来自磁盘）
  pub total: HashMap<WalId, Size>,
  /// Unflushed increments
  /// 未刷盘增量
  pub ing: HashMap<WalId, usize>,
}

impl CountInner {
  /// Create from loaded data
  /// 从加载的数据创建
  #[inline]
  pub fn from_loaded(total: HashMap<WalId, Size>) -> Self {
    Self {
      total,
      ing: HashMap::new(),
    }
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.ing.is_empty()
  }

  /// Add size increment for wal_id
  /// 为 wal_id 添加大小增量
  #[inline]
  pub fn add(&mut self, wal_id: WalId, size: usize) {
    *self.ing.entry(wal_id).or_default() += size;
  }

  /// Get total size for wal_id (disk + unflushed)
  /// 获取 wal_id 的全量大小（磁盘 + 未刷盘）
  #[inline]
  pub fn get(&self, wal_id: WalId) -> Size {
    let disk = self.total.get(&wal_id).copied().unwrap_or(0);
    let inc = self.ing.get(&wal_id).copied().unwrap_or(0) as Size;
    disk + inc
  }

  /// Remove wal_id (after GC)
  /// 移除 wal_id（GC 后）
  #[inline]
  pub fn remove(&mut self, wal_id: WalId) {
    self.total.remove(&wal_id);
    self.ing.remove(&wal_id);
  }

  /// Merge ing to total, return dirty wal_ids
  /// 合并 ing 到 total，返回脏 wal_id
  #[inline]
  pub fn merge(&mut self) -> Vec<WalId> {
    let dirty: Vec<_> = self.ing.keys().copied().collect();
    for (&wal_id, &inc) in &self.ing {
      *self.total.entry(wal_id).or_default() += inc as Size;
    }
    self.ing.clear();
    dirty
  }
}

impl Compact for CountInner {
  #[inline]
  fn compact_len(&self) -> usize {
    self.total.len()
  }

  #[inline]
  fn iter(&self) -> impl Iterator<Item = impl zbin::Bin<'_>> {
    self.total.iter().map(|(&wal_id, &size)| {
      to_disk_vec(KIND, &CountEntry::new(wal_id, size))
    })
  }
}

/// GC count with auto-compaction
/// 带自动压缩的 GC 统计
pub type Count = AutoCompact<CountInner>;

/// Load and create Count
/// 加载并创建 Count
pub async fn load(dir: &Path, count: usize) -> io::Result<Count> {
  let path = dir.join(COUNT_FILE);
  let map = jdb_fs::kv::load::<CountEntry>(&path).await?;
  let inner = CountInner::from_loaded(map);
  AutoCompact::new(inner, path, count, COMPACT_THRESHOLD).await
}

/// Flush dirty entries
/// 刷盘脏条目
pub async fn flush(count: &mut Count) -> io::Result<()> {
  let dirty = count.inner.merge();
  if dirty.is_empty() {
    return Ok(());
  }

  let data: Vec<_> = dirty
    .iter()
    .map(|&wal_id| {
      let size = count.inner.total.get(&wal_id).copied().unwrap_or(0);
      to_disk_vec(KIND, &CountEntry::new(wal_id, size))
    })
    .collect();

  count.save_iter(data).await?;
  count.maybe_compact().await?;
  Ok(())
}
