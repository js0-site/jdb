//! Memtable manager (active + frozen)
//! 内存表管理器（活跃 + 冻结）

use std::{cell::RefCell, collections::BTreeMap, mem as stdmem, ops::Bound, rc::Rc};

use jdb_base::{
  Pos,
  sst::{Flush, OnFlush},
};
use jdb_gc::GcLog;

use crate::{Key, Mem, MergeIter, MergeRevIter, flush, mem};

/// Default memtable size threshold (64MB, same as RocksDB)
/// 默认内存表大小阈值（64MB，与 RocksDB 相同）
pub const DEFAULT_MEM_SIZE: u64 = 64 * 1024 * 1024;

/// Memtable manager with async flush
/// 带异步刷盘的内存表管理器
pub struct Mems<F, N> {
  active: Mem,
  inner: Rc<RefCell<flush::Inner<F, N>>>,
  max_size: u64,
  gc_log: GcLog,
}

impl<F: Flush, N: OnFlush> Mems<F, N> {
  #[inline]
  pub fn new(flusher: F, notify: N, max_size: u64, gc_log: GcLog) -> Self {
    Self {
      active: mem::new(),
      inner: Rc::new(RefCell::new(flush::Inner {
        frozen: BTreeMap::new(),
        flusher: Some(flusher),
        notify,
        flushing: false,
        event: event_listener::Event::new(),
      })),
      max_size,
      gc_log,
    }
  }

  /// Put key-value pair, auto freeze if size exceeds threshold
  /// 插入键值对，超过阈值自动冻结
  #[inline]
  pub fn put(&mut self, key: impl Into<Key>, pos: Pos) {
    if pos.is_tombstone() {
      return;
    }
    Rc::make_mut(&mut self.active).put(key, pos, &self.gc_log);
    self.maybe_freeze();
  }

  /// Remove key (insert tombstone), auto freeze if size exceeds threshold
  /// 删除键（插入墓碑），超过阈值自动冻结
  #[inline]
  pub fn rm(&mut self, key: impl Into<Key>, old_pos: Pos) {
    Rc::make_mut(&mut self.active).rm(key, old_pos, &self.gc_log);
    self.maybe_freeze();
  }

  #[inline]
  fn maybe_freeze(&mut self) {
    if self.active.size >= self.max_size {
      self.freeze_inner();
      self.try_spawn_flush();
    }
  }

  fn freeze_inner(&mut self) {
    let old = stdmem::replace(&mut self.active, mem::new());
    if old.data.is_empty() {
      return;
    }
    let id = old.id;
    self.inner.borrow_mut().frozen.insert(id, old);
  }

  fn try_spawn_flush(&self) {
    flush::spawn(self.inner.clone());
  }

  /// Freeze active and flush all to disk
  /// 冻结活跃表并全部刷盘
  pub async fn flush(&mut self) {
    self.freeze_inner();
    flush::all(&self.inner).await;
  }

  /// Get value by key (active then frozen, newest first)
  /// 按键获取值（先活跃，再冻结，从新到旧）
  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<Pos> {
    if let Some(pos) = self.active.get(key) {
      return Some(pos);
    }
    // Iterate frozen maps from newest (largest ID) to oldest
    // 从最新（最大ID）到最旧遍历冻结表
    self
      .inner
      .borrow()
      .frozen
      .values()
      .rev()
      .find_map(|m| m.get(key))
  }

  /// Collect all mems (active + frozen) for iteration
  /// 收集所有 mems（活跃 + 冻结）用于迭代
  #[inline]
  fn all_mems(&self) -> impl Iterator<Item = Mem> {
    // Snapshot frozen mems (cheap Rc clone)
    // 快照冻结的 mems（廉价的 Rc 克隆）
    let inner = self.inner.borrow();
    let mut mems = Vec::with_capacity(inner.frozen.len() + 1);
    mems.push(self.active.clone());
    mems.extend(inner.frozen.values().rev().cloned());
    drop(inner);
    mems.into_iter()
  }

  pub(crate) fn merge_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeIter<'_> {
    MergeIter::new(self.all_mems(), start, end)
  }

  pub(crate) fn merge_rev_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeRevIter<'_> {
    MergeRevIter::new(self.all_mems(), start, end)
  }
}
