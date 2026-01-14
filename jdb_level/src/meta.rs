use std::{cell::Cell, cmp::Ordering, ops::Deref, rc::Rc};

use jdb_base::sst;

/// Inner data for Meta
/// Meta 的内部数据
#[derive(Debug)]
struct Inner {
  meta: sst::Meta,
  is_rm: Cell<bool>,
  lru: crate::Lru,
}

/// Meta wrapped with auto-removal logic
/// 带有自动删除逻辑的 Meta 包装
#[derive(Debug, Clone)]
pub struct Meta {
  inner: Rc<Inner>,
}

impl Meta {
  /// Create new Meta
  /// 创建新 Meta
  #[inline]
  pub fn new(meta: sst::Meta, lru: crate::Lru) -> Self {
    Self {
      inner: Rc::new(Inner {
        meta,
        is_rm: Cell::new(false),
        lru,
      }),
    }
  }

  /// Mark for removal on drop
  /// 标记在 drop 时删除
  #[inline]
  pub fn mark_rm(&self) {
    self.inner.is_rm.set(true);
  }
}

impl Drop for Meta {
  fn drop(&mut self) {
    // Only delete file when this is the last reference and marked for removal
    // 只有当这是最后一个引用且标记为删除时才删除文件
    if Rc::strong_count(&self.inner) == 1 && self.inner.is_rm.get() {
      self.inner.lru.borrow_mut().rm(self.inner.meta.id);
    }
  }
}

impl Deref for Meta {
  type Target = sst::Meta;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.inner.meta
  }
}

impl PartialEq for Meta {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.cmp(other).is_eq()
  }
}

impl Eq for Meta {}

impl PartialOrd for Meta {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Meta {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.inner.meta.cmp(&other.inner.meta)
  }
}

impl std::ops::RangeBounds<[u8]> for Meta {
  #[inline]
  fn start_bound(&self) -> std::ops::Bound<&[u8]> {
    self.inner.meta.start_bound()
  }

  #[inline]
  fn end_bound(&self) -> std::ops::Bound<&[u8]> {
    self.inner.meta.end_bound()
  }
}
