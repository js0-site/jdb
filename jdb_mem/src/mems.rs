//! Memtable manager (active + frozen)
//! 内存表管理器（活跃 + 冻结）

use std::rc::{Rc, Weak};

use jdb_base::Pos;

use crate::{Handle, Mem, handle::FrozenMap};

/// Memtable manager
/// 内存表管理器
pub struct Mems {
  active: Rc<Handle>,
  frozen: FrozenMap,
}

impl Mems {
  #[inline]
  pub fn new() -> Self {
    let frozen = FrozenMap::default();
    Self {
      active: Rc::new(Handle {
        mem: Mem::new(),
        frozen: Rc::clone(&frozen),
      }),
      frozen,
    }
  }

  #[inline]
  pub fn active(&self) -> &Rc<Handle> {
    &self.active
  }

  /// Get mutable active memtable
  /// 获取可变活跃内存表
  #[inline]
  pub fn active_mut(&mut self) -> &mut Mem {
    // SAFETY: only Mems holds active, no other Rc exists before freeze
    // 安全：freeze 前只有 Mems 持有 active
    Rc::get_mut(&mut self.active)
      .expect("active has other refs")
      .mem_mut()
  }

  #[inline]
  pub fn active_size(&self) -> u64 {
    self.active.mem.size()
  }

  /// Freeze active memtable, return handle
  /// 冻结活跃内存表，返回句柄
  pub fn freeze(&mut self) -> Rc<Handle> {
    let old = std::mem::replace(
      &mut self.active,
      Rc::new(Handle {
        mem: Mem::new(),
        frozen: Rc::clone(&self.frozen),
      }),
    );
    let id = old.mem.id();
    self.frozen.borrow_mut().insert(id, Rc::downgrade(&old));
    old
  }

  /// Get oldest frozen memtable (smallest id)
  /// 获取最旧的冻结内存表（最小 id）
  pub fn oldest_frozen(&self) -> Option<Rc<Handle>> {
    self
      .frozen
      .borrow()
      .iter()
      .filter_map(|(&id, w)| w.upgrade().map(|h| (id, h)))
      .min_by_key(|(id, _)| *id)
      .map(|(_, h)| h)
  }

  /// Get oldest frozen id
  /// 获取最旧的冻结 id
  pub fn oldest_frozen_id(&self) -> Option<u64> {
    self
      .frozen
      .borrow()
      .iter()
      .filter_map(|(&id, w)| w.upgrade().map(|_| id))
      .min()
  }

  #[inline]
  pub fn get_frozen(&self, id: u64) -> Option<Rc<Handle>> {
    self.frozen.borrow().get(&id).and_then(Weak::upgrade)
  }

  #[inline]
  pub fn has_frozen(&self) -> bool {
    !self.frozen.borrow().is_empty()
  }

  #[inline]
  pub fn frozen_count(&self) -> usize {
    self.frozen.borrow().len()
  }

  /// Get value by key (active then frozen, newest first)
  /// 按键获取值（先活跃，再冻结，从新到旧）
  pub fn get(&self, key: &[u8]) -> Option<Pos> {
    if let Some(pos) = self.active.mem.get(key) {
      return Some(pos);
    }
    // Sort by id desc (newest first)
    // 按 id 降序（从新到旧）
    let frozen = self.frozen.borrow();
    let mut handles: Vec<_> = frozen
      .iter()
      .filter_map(|(&id, w)| w.upgrade().map(|h| (id, h)))
      .collect();
    handles.sort_by(|a, b| b.0.cmp(&a.0));
    for (_, handle) in handles {
      if let Some(pos) = handle.mem.get(key) {
        return Some(pos);
      }
    }
    None
  }

  /// Collect all handles (active + frozen, newest first)
  /// 收集所有句柄（活跃 + 冻结，从新到旧）
  pub fn all_handles(&self) -> Vec<Rc<Handle>> {
    let mut handles = vec![Rc::clone(&self.active)];
    let frozen = self.frozen.borrow();
    let mut frozen_handles: Vec<_> = frozen
      .iter()
      .filter_map(|(&id, w)| w.upgrade().map(|h| (id, h)))
      .collect();
    frozen_handles.sort_by(|a, b| b.0.cmp(&a.0));
    handles.extend(frozen_handles.into_iter().map(|(_, h)| h));
    handles
  }

  /// Collect frozen handles (newest first)
  /// 收集冻结句柄（从新到旧）
  pub fn frozen_handles(&self) -> Vec<Rc<Handle>> {
    let frozen = self.frozen.borrow();
    let mut handles: Vec<_> = frozen
      .iter()
      .filter_map(|(&id, w)| w.upgrade().map(|h| (id, h)))
      .collect();
    handles.sort_by(|a, b| b.0.cmp(&a.0));
    handles.into_iter().map(|(_, h)| h).collect()
  }
}

impl Default for Mems {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}
