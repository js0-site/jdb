//! GC log - records discarded Pos for WAL GC
//! GC 日志 - 记录被丢弃的 Pos 用于 WAL GC

use std::{cell::RefCell, collections::HashMap, io, path::Path, rc::Rc};

use compio::io::AsyncWriteAtExt;
use jdb_base::{Pos, sst::OnDiscard};
use jdb_fs::{
  fs::open_read_write_create,
  fs_id::id_path,
  kv::{CRC_SIZE, HEAD_SIZE, to_disk_vec},
};

/// GC directory name
/// GC 目录名
pub const GC_DIR: &str = "gc/wal";

/// Pos entry kind
/// Pos 条目类型
const KIND_POS: u8 = 2;

/// Pos disk size
/// Pos 磁盘大小
const POS_DISK_SIZE: usize = HEAD_SIZE + size_of::<Pos>() + CRC_SIZE;

/// Inner data
/// 内部数据
#[derive(Default)]
struct Inner {
  by_wal: HashMap<u64, Vec<Pos>>,
}

/// GC log - collects discarded Pos grouped by wal_id (with Rc<RefCell> inside)
/// GC 日志 - 按 wal_id 分组收集被丢弃的 Pos（内置 Rc<RefCell>）
#[derive(Clone, Default)]
pub struct GcLog {
  inner: Rc<RefCell<Inner>>,
}

impl GcLog {
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.inner.borrow().by_wal.is_empty()
  }

  /// Total discarded count
  /// 总丢弃数量
  #[inline]
  pub fn count(&self) -> usize {
    self.inner.borrow().by_wal.values().map(|v| v.len()).sum()
  }

  /// Record discarded pos
  /// 记录被丢弃的 pos
  #[inline]
  pub fn discard(&self, pos: Pos) {
    self
      .inner
      .borrow_mut()
      .by_wal
      .entry(pos.wal_id())
      .or_default()
      .push(pos);
  }

  /// Record discarded pos with key (for OnDiscard trait)
  /// 记录被丢弃的 pos（用于 OnDiscard trait）
  #[inline]
  pub fn discard_kv(&self, _key: &[u8], pos: &Pos) {
    self
      .inner
      .borrow_mut()
      .by_wal
      .entry(pos.wal_id())
      .or_default()
      .push(*pos);
  }

  /// Iterate by wal_id (takes closure to avoid borrow issues)
  /// 按 wal_id 迭代（使用闭包避免借用问题）
  #[inline]
  pub fn for_each<F: FnMut(u64, &Vec<Pos>)>(&self, mut f: F) {
    for (&wal_id, positions) in &self.inner.borrow().by_wal {
      f(wal_id, positions);
    }
  }

  /// Flush discarded positions to gc/{wal_id} files
  /// 将丢弃的位置刷盘到 gc/{wal_id} 文件
  pub async fn flush(&self, dir: &Path) -> io::Result<()> {
    if self.is_empty() {
      return Ok(());
    }

    let gc_dir = dir.join(GC_DIR);
    compio::fs::create_dir_all(&gc_dir).await?;

    let by_wal = self.inner.borrow();
    for (&wal_id, positions) in by_wal.by_wal.iter() {
      if positions.is_empty() {
        continue;
      }

      let path = id_path(&gc_dir, wal_id);
      let mut file = open_read_write_create(&path).await?;

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
}

impl OnDiscard for GcLog {
  #[inline]
  fn discard(&mut self, key: &[u8], pos: &Pos) {
    self.discard_kv(key, pos);
  }
}

impl OnDiscard for &GcLog {
  #[inline]
  fn discard(&mut self, key: &[u8], pos: &Pos) {
    self.discard_kv(key, pos);
  }
}
