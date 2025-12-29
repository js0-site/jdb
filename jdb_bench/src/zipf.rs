// Zipf distribution workload generator
// Zipf 分布工作负载生成器

/// Zipf distribution sampler with precomputed CDF
/// 预计算 CDF 的 Zipf 分布采样器
pub struct ZipfSampler {
  /// Precomputed cumulative distribution / 预计算的累积分布
  cdf: Vec<f64>,
}

impl ZipfSampler {
  /// Create new Zipf sampler with precomputed CDF for O(log n) sampling
  /// 创建预计算 CDF 的 Zipf 采样器，采样复杂度 O(log n)
  pub fn new(n: usize, s: f64) -> Self {
    if n == 0 {
      return Self { cdf: Vec::new() };
    }
    // Precompute CDF: cdf[k] = sum(1/i^s for i in 1..=k+1) / H_{n,s}
    // 预计算 CDF
    let mut cdf = Vec::with_capacity(n);
    let mut sum = 0.0;
    for k in 1..=n {
      sum += 1.0 / (k as f64).powf(s);
      cdf.push(sum);
    }
    // Normalize / 归一化
    let h_ns = sum;
    for v in &mut cdf {
      *v /= h_ns;
    }
    Self { cdf }
  }

  /// Sample a 0-based index using binary search O(log n)
  /// 使用二分查找采样，复杂度 O(log n)
  #[inline]
  pub fn sample(&self, rng: &mut fastrand::Rng) -> usize {
    if self.cdf.is_empty() {
      return 0;
    }
    let u = rng.f64();
    // Binary search for first cdf[i] >= u
    // 二分查找第一个 cdf[i] >= u
    self.cdf.partition_point(|&v| v < u)
  }
}

/// Zipf-based workload generator holding key-value pairs
/// 基于 Zipf 分布的工作负载生成器，持有键值对
pub struct ZipfWorkload<K, V> {
  data: Vec<(K, V)>,
  sampler: ZipfSampler,
  rng: fastrand::Rng,
}

impl<K, V> ZipfWorkload<K, V> {
  /// Create new Zipf workload with given data and exponent
  /// 使用给定数据和指数创建新的 Zipf 工作负载
  ///
  /// # Arguments
  /// - `data`: Key-value pairs / 键值对
  /// - `s`: Zipf exponent (typically 1.0-2.0, higher = more skewed) / Zipf 指数
  /// - `seed`: Random seed for reproducibility / 随机种子
  pub fn new(data: Vec<(K, V)>, s: f64, seed: u64) -> Self {
    let n = data.len();
    let sampler = ZipfSampler::new(n, s);
    let rng = fastrand::Rng::with_seed(seed);
    Self { data, sampler, rng }
  }

  /// Get data length / 获取数据长度
  #[inline]
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Check if empty / 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  /// Sample index using Zipf distribution
  /// 使用 Zipf 分布采样索引
  #[inline]
  fn sample_idx(&mut self) -> usize {
    self.sampler.sample(&mut self.rng)
  }

  /// Get key by Zipf distribution
  /// 按 Zipf 分布返回 key
  #[inline]
  pub fn key(&mut self) -> Option<&K> {
    if self.data.is_empty() {
      return None;
    }
    let idx = self.sample_idx();
    Some(&self.data[idx].0)
  }

  /// Get key-value pair by Zipf distribution
  /// 按 Zipf 分布返回 key-value 对
  #[inline]
  pub fn key_val(&mut self) -> Option<(&K, &V)> {
    if self.data.is_empty() {
      return None;
    }
    let idx = self.sample_idx();
    let (k, v) = &self.data[idx];
    Some((k, v))
  }

  /// Get mutable key-value pair by Zipf distribution
  /// 按 Zipf 分布返回可变 key-value 对
  #[inline]
  pub fn key_val_mut(&mut self) -> Option<(&mut K, &mut V)> {
    if self.data.is_empty() {
      return None;
    }
    let idx = self.sample_idx();
    let (k, v) = &mut self.data[idx];
    Some((k, v))
  }

  /// Get underlying data reference
  /// 获取底层数据引用
  #[inline]
  pub fn data(&self) -> &[(K, V)] {
    &self.data
  }

  /// Reset with new seed for reproducibility
  /// 使用新种子重置以实现可重现性
  pub fn reset(&mut self, seed: u64) {
    self.rng = fastrand::Rng::with_seed(seed);
  }

  /// Sample n indices and return frequency distribution (for testing)
  /// 采样 n 个索引并返回频率分布（用于测试）
  pub fn sample_distribution(&mut self, n: usize) -> Vec<usize> {
    let len = self.data.len();
    if len == 0 {
      return Vec::new();
    }
    let mut counts = vec![0usize; len];
    for _ in 0..n {
      let idx = self.sample_idx();
      counts[idx] += 1;
    }
    counts
  }
}

/// Convenience type for byte key-value workload
/// 字节键值工作负载的便捷类型
pub type ByteZipfWorkload = ZipfWorkload<Vec<u8>, Vec<u8>>;

/// Convenience type for string key-value workload
/// 字符串键值工作负载的便捷类型
pub type StrZipfWorkload = ZipfWorkload<String, String>;
