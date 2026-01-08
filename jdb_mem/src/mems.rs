//! Memtable manager (active + frozen)
//! 内存表管理器（活跃 + 冻结）

use std::{collections::BTreeMap, ops::Bound, rc::Rc};

use jdb_base::Pos;

use crate::{Mem, MemInner, MergeIter, MergeRevIter, mem};

/// Memtable manager
/// 内存表管理器
#[derive(Default)]
pub struct Mems {
  active: Mem,
  frozen: BTreeMap<u64, Mem>,
}

impl Mems {
  #[inline]
  pub fn new() -> Self {
    Self {
      active: mem::new(),
      frozen: BTreeMap::new(),
    }
  }

  /// Get mutable active memtable
  /// 获取可变活跃内存表
  ///
  /// Uses Copy-On-Write: clones if shared
  /// 使用写时复制：如果被共享则克隆
  #[inline]
  pub fn active_mut(&mut self) -> &mut MemInner {
    Rc::make_mut(&mut self.active)
  }

  #[inline]
  pub fn active_size(&self) -> u64 {
    self.active.size
  }

  /// Freeze active memtable, return handle
  /// 冻结活跃内存表，返回句柄
  pub fn freeze(&mut self) -> Mem {
    let old = std::mem::replace(&mut self.active, mem::new());
    let id = old.id;
    let m = old.clone();
    self.frozen.insert(id, old);
    m
  }

  #[inline]
  pub fn has_frozen(&self) -> bool {
    !self.frozen.is_empty()
  }

  #[inline]
  pub fn frozen_count(&self) -> usize {
    self.frozen.len()
  }

  /// Remove frozen memtable by id
  /// 按 id 移除冻结的内存表
  #[inline]
  pub fn rm_frozen(&mut self, id: u64) {
    self.frozen.remove(&id);
  }

  /// Get value by key (active then frozen, newest first)
  /// 按键获取值（先活跃，再冻结，从新到旧）
  pub fn get(&self, key: &[u8]) -> Option<Pos> {
    if let Some(pos) = self.active.get(key) {
      return Some(pos);
    }
    for handle in self.frozen.values().rev() {
      if let Some(pos) = handle.get(key) {
        return Some(pos);
      }
    }
    None
  }

  /// Forward merge iterator
  /// 正向归并迭代器
  pub(crate) fn merge_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeIter<'_> {
    // Use iterator chain to avoid allocating intermediate Vec
    // 使用迭代器链避免分配中间 Vec
    let iter = std::iter::once(self.active.clone()).chain(self.frozen.values().rev().cloned());
    MergeIter::new(iter, start, end)
  }

  /// Reverse merge iterator
  /// 反向归并迭代器
  pub(crate) fn merge_rev_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeRevIter<'_> {
    let iter = std::iter::once(self.active.clone()).chain(self.frozen.values().rev().cloned());
    MergeRevIter::new(iter, start, end)
  }
}
