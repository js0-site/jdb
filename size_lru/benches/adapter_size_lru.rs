// size_lru adapter / size_lru 适配器

use crate::common::LruBench;

/// size_lru::Lhd adapter with weight = key.len() + val.len()
/// size_lru::Lhd 适配器，权重 = key.len() + val.len()
pub struct SizeLruAdapter {
  cache: size_lru::Lhd<Vec<u8>, Vec<u8>>,
}

impl SizeLruAdapter {
  pub fn new(cap: u64) -> Self {
    Self {
      cache: size_lru::Lhd::new(cap as usize),
    }
  }
}

impl LruBench for SizeLruAdapter {
  fn name(&self) -> &'static str {
    "size_lru"
  }

  fn set(&mut self, key: &[u8], val: &[u8]) {
    let weight = (key.len() + val.len()) as u32;
    self.cache.set(key.to_vec(), val.to_vec(), weight);
  }

  fn get(&mut self, key: &[u8]) -> bool {
    self.cache.get(&key.to_vec()).is_some()
  }
}
