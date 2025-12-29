//! NoCache - zero overhead no-op cache
//! NoCache - 零开销空操作缓存

use crate::SizeLru;

/// No-op cache
/// 空操作缓存
pub struct NoCache;

impl<K, V> SizeLru<K, V> for NoCache {
  #[inline(always)]
  fn get(&mut self, _: &K) -> Option<&V> {
    None
  }

  #[inline(always)]
  fn set(&mut self, _: K, _: V, _: u32) {}

  #[inline(always)]
  fn rm(&mut self, _: &K) {}
}
