//! GC log - records discarded entries for WAL GC
//! GC 日志 - 记录被丢弃的条目用于 WAL GC

mod count;

use std::{
  collections::HashMap,
  io,
  path::{Path, PathBuf},
};

use compio::io::AsyncWriteAtExt;
pub use count::{Count, CountInner, Size, flush as count_flush, load as count_load};
use jdb_base::{Pos, WalId, entry_size, sst::OnDiscard};
use jdb_fs::{
  fs::open_read_write_create,
  fs_id::id_path,
  kv::{CRC_SIZE, HEAD_SIZE, to_disk_vec},
};

/// GC directory
/// GC 目录
pub const GC_DIR: &str = "gc/wal";

/// Pos entry kind
/// Pos 条目类型
const KIND_POS: u8 = 2;

/// Pos disk size
/// Pos 磁盘大小
const POS_DISK_SIZE: usize = HEAD_SIZE + size_of::<Pos>() + CRC_SIZE;

/// Key length type
/// Key 长度类型
pub type KeyLen = u16;

/// GC log - collects discarded entries grouped by wal_id
/// GC 日志 - 按 wal_id 分组收集被丢弃的条目
pub struct GcLog {
  dir: PathBuf,
  /// Pos entries grouped by wal_id
  /// 按 wal_id 分组的 Pos 条目
  inner: HashMap<WalId, Vec<(KeyLen, Pos)>>,
  /// Total count with auto-compaction
  /// 带自动压缩的全量统计
  pub count: Count,
}

impl GcLog {
  /// Create with loaded count
  /// 使用加载的统计创建
  #[inline]
  pub fn new(dir: impl Into<PathBuf>, count: Count) -> Self {
    Self {
      dir: dir.into(),
      inner: HashMap::new(),
      count,
    }
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.inner.is_empty() && self.count.inner.is_empty()
  }

  /// Add discarded entry
  /// 添加被丢弃的条目
  #[inline]
  pub fn add(&mut self, key: &[u8], pos: Pos) {
    let wal_id = pos.wal_id();
    let key_len = key.len();
    self
      .inner
      .entry(wal_id)
      .or_default()
      .push((key_len as KeyLen, pos));
    self.count.inner.add(wal_id, entry_size(key_len, pos.len() as usize));
  }

  /// Flush to disk concurrently
  /// 并发刷盘
  pub async fn flush(&mut self) -> io::Result<()> {
    if self.is_empty() {
      return Ok(());
    }

    let items = std::mem::take(&mut self.inner);

    let gc_dir = self.dir.join(GC_DIR);
    compio::fs::create_dir_all(&gc_dir).await?;

    // Flush pos items and count concurrently
    // 并发刷盘 pos 条目和统计
    let count_task = count_flush(&mut self.count);
    let items_task = flush_items(&gc_dir, items);

    let (count_res, items_res) = futures::future::join(count_task, items_task).await;
    count_res?;
    items_res?;

    Ok(())
  }
}

/// Flush pos items to files
/// 刷盘 pos 条目到文件
async fn flush_items(gc_dir: &Path, items: HashMap<WalId, Vec<(KeyLen, Pos)>>) -> io::Result<()> {
  for (wal_id, entries) in items {
    if entries.is_empty() {
      continue;
    }

    let path = id_path(gc_dir, wal_id);
    let mut file = open_read_write_create(&path).await?;
    let offset = file.metadata().await?.len();

    let mut buf = Vec::with_capacity(entries.len() * POS_DISK_SIZE);
    for (_, pos) in &entries {
      buf.extend_from_slice(&to_disk_vec(KIND_POS, pos));
    }

    file.write_all_at(buf, offset).await.0?;
    file.sync_all().await?;
  }

  Ok(())
}

impl OnDiscard for GcLog {
  #[inline]
  fn discard(&mut self, key: &[u8], pos: &Pos) {
    self.add(key, *pos);
  }
}
