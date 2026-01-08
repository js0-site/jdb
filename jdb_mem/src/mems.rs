//! Memtable manager (active + frozen)
//! 内存表管理器（活跃 + 冻结）

use std::rc::Rc;

use jdb_base::Pos;

use crate::{handle::FrozenMap, Handle, Mem};

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
    let ptr = Rc::as_ptr(&old);
    self.frozen.borrow_mut().insert(id, ptr);
    old
  }

  /// Get oldest frozen memtable (smallest id)
  /// 获取最旧的冻结内存表（最小 id）
  pub fn oldest_frozen(&self) -> Option<Rc<Handle>> {
    // BTreeMap is sorted by key, first value is the oldest (smallest id)
    // BTreeMap 按 key 排序，第一个值即为最旧的
    self.frozen.borrow().values().next().map(|&ptr| {
      // SAFETY: ptr valid while in map, drop removes it
      // 安全：指针在 map 中时有效，drop 时移除
      unsafe { Rc::increment_strong_count(ptr) };
      unsafe { Rc::from_raw(ptr) }
    })
  }

  /// Get oldest frozen id
  /// 获取最旧的冻结 id
  pub fn oldest_frozen_id(&self) -> Option<u64> {
    // BTreeMap keys are sorted
    self.frozen.borrow().keys().next().copied()
  }

  #[inline]
  pub fn get_frozen(&self, id: u64) -> Option<Rc<Handle>> {
    self.frozen.borrow().get(&id).map(|&ptr| {
      // SAFETY: ptr valid while in map
      // 安全：指针在 map 中时有效
      unsafe { Rc::increment_strong_count(ptr) };
      unsafe { Rc::from_raw(ptr) }
    })
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
    for &ptr in self.frozen.borrow().values().rev() {
      // SAFETY: ptr valid while in map, RefCell borrow ensures no mutation
      // 安全：指针在 map 中时有效，RefCell 借用保证无修改
      let handle = unsafe { &*ptr };
      if let Some(pos) = handle.mem.get(key) {
        return Some(pos);
      }
    }
    None
  }

  /// Collect all handles (active + frozen, newest first)
  /// 收集所有句柄（活跃 + 冻结，从新到旧）
  pub fn all_handles(&self) -> Vec<Rc<Handle>> {
    let mut handles = self.frozen_handles();
    handles.insert(0, Rc::clone(&self.active));
    handles
  }

  /// Collect frozen handles (newest first)
  /// 收集冻结句柄（从新到旧），用于归并
  pub fn frozen_handles(&self) -> Vec<Rc<Handle>> {
    let frozen = self.frozen.borrow();
    let mut handles = Vec::with_capacity(frozen.len());
    
    // values().rev() gives descending order by ID (newest first)
    for &ptr in frozen.values().rev() {
      // SAFETY: ptr valid while in map
      // 安全：指针在 map 中时有效
      unsafe { Rc::increment_strong_count(ptr) };
      handles.push(unsafe { Rc::from_raw(ptr) });
    }
    handles
  }
}

impl Default for Mems {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}
