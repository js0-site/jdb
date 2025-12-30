// lru adapter / lru 适配器
// Item count based capacity (no weight support)
// 基于条目数的容量（不支持权重）

use std::num::NonZeroUsize;

use crate::common::LruBench;

/// lru::LruCache adapter with item count capacity
/// lru::LruCache 适配器，使用条目数容量
pub struct LruAdapter {
  cache: lru::LruCache<Vec<u8>, Vec<u8>>,
}

impl LruAdapter {
  pub fn new(cap: usize) -> Self {
    Self {
      cache: lru::LruCache::new(NonZeroUsize::new(cap).expect("cap > 0")),
    }
  }
}

impl LruBench for LruAdapter {
  fn name(&self) -> &'static str {
    "lru"
  }

  fn set(&mut self, key: &[u8], val: &[u8]) {
    self.cache.put(key.to_vec(), val.to_vec());
  }

  fn get(&mut self, key: &[u8]) -> bool {
    self.cache.get(&key.to_vec()).is_some()
  }
}
