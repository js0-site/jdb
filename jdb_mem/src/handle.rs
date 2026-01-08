//! Mem handle with auto-cleanup
//! 带自动清理的 Mem 句柄

use std::{
  cell::RefCell,
  collections::HashMap,
  ops::Deref,
  rc::{Rc, Weak},
};

use crate::Mem;

pub(crate) type FrozenMap = Rc<RefCell<HashMap<u64, Weak<Handle>>>>;

/// Handle for auto-cleanup on drop
/// 自动清理的句柄
pub struct Handle {
  pub mem: Mem,
  pub(crate) frozen: FrozenMap,
}

impl Drop for Handle {
  fn drop(&mut self) {
    self.frozen.borrow_mut().remove(&self.mem.id());
  }
}

impl Handle {
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
