//! Snapshot of Levels at a specific version
//! 特定版本的 Levels 快照

use std::{cell::RefCell, rc::Rc};

use jdb_base::table::Meta;

use crate::{Level, RefCountMap};

/// Snapshot of Levels at a specific version
/// 特定版本的 Levels 快照
pub struct Snapshot<T> {
  ver: u64,
  /// SSTable references by level
  /// 按层级的 SSTable 引用
  tables: Vec<Vec<Rc<T>>>,
  /// Shared refcount map for decrement on drop
  /// 共享引用计数映射，用于 drop 时递减
  refmap: Rc<RefCell<RefCountMap>>,
}

impl<T: Meta + Clone> Snapshot<T> {
  /// Create snapshot from current levels state
  /// 从当前 levels 状态创建快照
  pub fn new(ver: u64, levels: &[Level<T>], refmap: Rc<RefCell<RefCountMap>>) -> Self {
    refmap.borrow_mut().inc(ver);

    let tables = levels.iter().map(|l| l.iter_rc().collect()).collect();

    Self {
      ver,
      tables,
      refmap,
    }
  }

  /// Get snapshot version
  /// 获取快照版本
  #[inline]
  pub fn ver(&self) -> u64 {
    self.ver
  }

  /// Get tables at level
  /// 获取指定层的表
  #[inline]
  pub fn level(&self, n: u8) -> &[Rc<T>] {
    self
      .tables
      .get(n as usize)
      .map(|v| v.as_slice())
      .unwrap_or(&[])
  }

  /// Iterate all tables (L0 first, newest to oldest)
  /// 迭代所有表（L0 优先，从新到旧）
  pub fn iter(&self) -> impl Iterator<Item = &Rc<T>> {
    self.tables.iter().flat_map(|l| l.iter())
  }
}

impl<T> Clone for Snapshot<T> {
  fn clone(&self) -> Self {
    self.refmap.borrow_mut().inc(self.ver);
    Self {
      ver: self.ver,
      tables: self.tables.clone(),
      refmap: Rc::clone(&self.refmap),
    }
  }
}

impl<T> Drop for Snapshot<T> {
  fn drop(&mut self) {
    self.refmap.borrow_mut().dec(self.ver);
  }
}
