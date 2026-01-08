//! Mem handle with auto-cleanup
//! 带自动清理的 Mem 句柄

use std::{cell::RefCell, collections::BTreeMap, ops::Deref, rc::Rc};

use crate::Mem;

/// Map storing raw pointers to handles, sorted by ID
/// 存储句柄裸指针的映射，按 ID 排序
pub(crate) type FrozenMap = Rc<RefCell<BTreeMap<u64, *const Handle>>>;

/// Handle for auto-cleanup on drop
/// 自动清理的句柄，持有 Mem 的引用计数
#[derive(Debug)]
pub struct Handle {
  pub mem: Mem,
  pub(crate) frozen: FrozenMap,
}

// SAFETY: Drop handled carefully to avoid RefCell panic
impl Drop for Handle {
  fn drop(&mut self) {
    // Safety: RefCell might panic if we are iterating frozen map and dropping at the same time.
    // However, in single-threaded Rc context, this is deterministic.
    // 安全：如果在遍历 frozen map 的同时进行 drop，RefCell 可能会 panic。
    // 但在单线程 Rc 上下文中，这是确定性的。
    // 使用 try_borrow_mut 避免 panic，如果正在遍历（借用中），则跳过清理（极少数情况）
    if let Ok(mut map) = self.frozen.try_borrow_mut() {
      map.remove(&self.mem.id());
    }
  }
}

impl Handle {
  /// Get mutable reference to Mem
  #[inline]
  pub fn mem_mut(&mut self) -> &mut Mem {
    &mut self.mem
  }
}

impl Deref for Handle {
  type Target = Mem;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.mem
  }
}
