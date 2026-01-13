use std::{cell::Cell, cmp::Ordering, ops::Deref, path::PathBuf, rc::Rc};

use jdb_base::sst::Meta;

/// Meta wrapped with auto-removal logic
/// 带有自动删除逻辑的 Meta 包装
#[derive(Debug)]
pub struct MetaWithAutoRm {
  pub inner: Rc<Meta>,
  pub is_rm: Cell<bool>,
  pub dir: Rc<PathBuf>,
}

impl Deref for MetaWithAutoRm {
  type Target = Meta;
  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl PartialEq for MetaWithAutoRm {
  fn eq(&self, other: &Self) -> bool {
    self.inner == other.inner
  }
}

impl Eq for MetaWithAutoRm {}

impl PartialOrd for MetaWithAutoRm {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MetaWithAutoRm {
  fn cmp(&self, other: &Self) -> Ordering {
    self.inner.cmp(&other.inner)
  }
}

impl Drop for MetaWithAutoRm {
  fn drop(&mut self) {
    if self.is_rm.get() {
      let path = self.dir.join(format!("{}.sst", self.inner.id));
      // Ignore errors during drop, as we can't propagate them
      let _ = std::fs::remove_file(path);
    }
  }
}

impl std::ops::RangeBounds<[u8]> for MetaWithAutoRm {
  fn start_bound(&self) -> std::ops::Bound<&[u8]> {
    self.inner.start_bound()
  }

  fn end_bound(&self) -> std::ops::Bound<&[u8]> {
    self.inner.end_bound()
  }
}
