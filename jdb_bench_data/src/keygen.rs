// Key generator for benchmark
// 基准测试的 key 生成器

use crate::ZipfSampler;

/// Default Zipf exponent / 默认 Zipf 指数
pub const ZIPF_S: f64 = 1.2;

/// Default random seed / 默认随机种子
pub const SEED: u64 = 42;

/// Key expansion factor / key 扩容因子
pub const EXPAND: u32 = 250;

/// Key generator with Zipf distribution
/// Zipf 分布的 key 生成器
pub struct KeyGen {
  zipf: ZipfSampler,
  rng: fastrand::Rng,
  expand: u32,
}

impl KeyGen {
  /// Create with item count and default params
  /// 使用条目数和默认参数创建
  pub fn new(n: usize) -> Self {
    Self::with_params(n, ZIPF_S, SEED, EXPAND)
  }

  /// Create with custom params
  /// 使用自定义参数创建
  pub fn with_params(n: usize, s: f64, seed: u64, expand: u32) -> Self {
    Self {
      zipf: ZipfSampler::new(n, s),
      rng: fastrand::Rng::with_seed(seed),
      expand,
    }
  }

  /// Reset to initial state
  /// 重置到初始状态
  pub fn reset(&mut self, seed: u64) {
    self.rng = fastrand::Rng::with_seed(seed);
  }

  /// Sample next (item_idx, expand_id) pair
  /// 采样下一个 (item_idx, expand_id) 对
  #[inline]
  pub fn sample(&mut self) -> (usize, u32) {
    let idx = self.zipf.sample(&mut self.rng);
    let id = self.rng.u32(..self.expand);
    (idx, id)
  }

  /// Build full key from base key and expand_id
  /// 从基础 key 和 expand_id 构建完整 key
  #[inline]
  pub fn build_key(base: &[u8], id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(base.len() + 4);
    key.extend_from_slice(base);
    key.extend_from_slice(&id.to_le_bytes());
    key
  }

  /// Sample next full key from items
  /// 从 items 采样下一个完整 key
  #[inline]
  pub fn next_key<K: AsRef<[u8]>, V>(&mut self, items: &[(K, V)]) -> Vec<u8> {
    let (idx, id) = self.sample();
    Self::build_key(items[idx].0.as_ref(), id)
  }

  /// Sample next key-value pair
  /// 采样下一个 key-value 对
  #[inline]
  pub fn next_kv<'a, K: AsRef<[u8]>, V>(&mut self, items: &'a [(K, V)]) -> (Vec<u8>, &'a V) {
    let (idx, id) = self.sample();
    let (k, v) = &items[idx];
    (Self::build_key(k.as_ref(), id), v)
  }
}
