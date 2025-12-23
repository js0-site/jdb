#![cfg_attr(docsrs, feature(doc_cfg))]

use xorf::{BinaryFuse8, Filter as XorfFilter};

/// Binary Fuse Filter 封装
/// Binary Fuse Filter wrapper
pub struct Filter(BinaryFuse8);

impl Filter {
  /// 从 u64 key 列表构建 Build from u64 keys
  #[inline]
  pub fn new(keys: &[u64]) -> Option<Self> {
    BinaryFuse8::try_from(keys).ok().map(Self)
  }

  /// 检查 key 是否可能存在 Check if key may exist
  #[inline]
  pub fn may_contain(&self, key: u64) -> bool {
    self.0.contains(&key)
  }

  /// 内存占用 Memory usage in bytes
  #[inline]
  pub fn size(&self) -> usize {
    self.0.fingerprints.len()
  }
}
