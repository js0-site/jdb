//! LHD (Least Hit Density) cache for variable-sized data
//! LHD（最低命中密度）缓存，用于变长数据
//!
//! Based on: Beckmann et al. "LHD: Improving Cache Hit Rate by Maximizing Hit Density" (NSDI 2018)
//! 基于论文：Beckmann 等人的 LHD 算法
//!
//! Core idea: evict items with lowest expected_hits / size
//! 核心思想：淘汰 预期命中数/大小 最低的条目

use std::{borrow::Borrow, hash::Hash};

use fastrand::Rng;
use gxhash::{HashMap, HashMapExt};

use crate::SizeLru;

// Number of candidates to sample for eviction
// 淘汰时采样的候选者数量
const SAMPLES: usize = 64;

// Age classes for hit pattern classification
// 命中模式分类的年龄类别数
const AGE_CLASSES: usize = 16;

// Max age buckets for statistics
// 统计用的最大年龄桶数
// Note: MAX_AGE (4096) fits in u16 (max 65535)
// 注意：MAX_AGE (4096) 可安全转为 u16
const MAX_AGE: usize = 4096;

// Total buckets count (flattened)
// 总桶数（扁平化）
const TOTAL_BUCKETS: usize = AGE_CLASSES * MAX_AGE;

// Per-entry fixed overhead: Entry struct + HashMap entry
// 每条目固定开销
const ENTRY_OVERHEAD: u32 = 96;

// EWMA decay factor
// EWMA 衰减因子
const DECAY: f32 = 0.9;

// Reconfigure interval (access count)
// 重配置间隔（访问次数）
const RECONFIG: u64 = 1 << 15;

// Age coarsening divisor: 0.01 * MAX_AGE
// 年龄粗化除数
const AGE_DIVISOR: f32 = 40.96;

/// Hot metadata for eviction sampling (SoA layout)
/// 用于淘汰采样的热点元数据（SoA 布局）
/// Size: 8 + 4 + 2 + 2 = 16 bytes (4 items per cache line)
/// 大小：16字节（每缓存行可存4个）
#[derive(Clone, Copy)]
#[repr(C)]
struct Meta {
  ts: u64,
  size: u32,
  last_age: u16,
  prev_age: u16,
}

/// Cold payload data
/// 冷数据载荷
struct Payload<K, V> {
  key: K,
  val: V,
}

/// Per-age bucket statistics
/// 每个年龄桶的统计数据
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Bucket {
  hits: f32,
  evicts: f32,
  density: f32,
}

/// LHD cache with random sampling eviction
/// 带随机采样淘汰的 LHD 缓存
#[must_use]
pub struct Lhd<K, V> {
  // Split into hot/cold vectors for better cache locality during eviction
  // 拆分为热/冷向量，以在淘汰期间获得更好的缓存局部性
  metas: Vec<Meta>,
  payloads: Vec<Payload<K, V>>,
  index: HashMap<K, usize>,
  // Flattened statistics buckets
  // 扁平化的统计桶
  buckets: Box<[Bucket]>,
  total: usize,
  max: usize,
  ts: u64,
  shift: u32,
  last_cfg: u64,
  rng: Rng,
}

impl<K: Hash + Eq, V> Lhd<K, V> {
  /// Create with max memory in bytes
  /// 创建，指定最大内存（字节）
  pub fn new(max: usize) -> Self {
    // Allocate all buckets in a single contiguous memory block
    // 在单个连续内存块中分配所有桶
    let mut buckets = vec![Bucket::default(); TOTAL_BUCKETS].into_boxed_slice();

    // Initialize densities to GDSF-like: density ~ 1/age
    // 初始化密度为类 GDSF 分布：密度 ~ 1/age
    for cid in 0..AGE_CLASSES {
      let offset = cid * MAX_AGE;
      for i in 0..MAX_AGE {
        unsafe {
          buckets.get_unchecked_mut(offset + i).density = 1.0 / (i as f32 + 1.0);
        }
      }
    }

    Self {
      metas: Vec::new(),
      payloads: Vec::new(),
      index: HashMap::new(),
      buckets,
      total: 0,
      max: max.max(1),
      ts: 0,
      shift: 6,
      last_cfg: 0,
      rng: Rng::new(),
    }
  }

  /// Get value and update statistics
  /// 获取值并更新统计
  #[inline]
  pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    self.tick();
    let &idx = self.index.get(key)?;
    let ts = self.ts;
    let shift = self.shift;

    // Access hot metadata
    // 访问热元数据
    let m = unsafe { self.metas.get_unchecked_mut(idx) };
    let age = ((ts.saturating_sub(m.ts) >> shift) as usize).min(MAX_AGE - 1);
    let cid = Self::class_id(m.last_age as u32 + m.prev_age as u32);

    // Update metadata
    m.prev_age = m.last_age;
    m.last_age = age as u16;
    m.ts = ts;

    // Update stats
    unsafe {
      self.buckets.get_unchecked_mut(cid * MAX_AGE + age).hits += 1.0;
    }

    // Access cold payload
    // 访问冷载荷
    Some(&unsafe { self.payloads.get_unchecked(idx) }.val)
  }

  /// Insert with size
  /// 插入并指定大小
  #[inline]
  pub fn set(&mut self, key: K, val: V, size: u32)
  where
    K: Clone,
  {
    self.tick();
    let size = size + ENTRY_OVERHEAD;
    let sz = size as usize;

    // Update existing entry
    if let Some(&idx) = self.index.get(&key) {
      let m = unsafe { self.metas.get_unchecked_mut(idx) };
      self.total = self.total.wrapping_sub(m.size as usize).wrapping_add(sz);
      m.size = size;
      m.ts = self.ts;
      // Update value in payload
      unsafe { self.payloads.get_unchecked_mut(idx) }.val = val;
      return;
    }

    // Evict if necessary
    while self.total + sz > self.max && !self.metas.is_empty() {
      self.evict();
    }

    // Insert new entry
    let idx = self.metas.len();
    self.index.insert(key.clone(), idx);

    // Push split data
    self.metas.push(Meta {
      ts: self.ts,
      size,
      last_age: 0,
      prev_age: MAX_AGE as u16,
    });
    self.payloads.push(Payload { key, val });
    self.total += sz;
  }

  /// Remove by key
  #[inline]
  pub fn rm<Q>(&mut self, key: &Q)
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    if let Some(idx) = self.index.remove(key) {
      self.rm_internal(idx);
    }
  }

  #[inline]
  pub fn size(&self) -> usize {
    self.total
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.metas.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.metas.is_empty()
  }

  #[inline(always)]
  fn tick(&mut self) {
    self.ts = self.ts.wrapping_add(1);
    if self.ts.wrapping_sub(self.last_cfg) >= RECONFIG {
      self.reconfig();
    }
  }

  #[inline(always)]
  fn age(&self, ts: u64) -> usize {
    ((self.ts.saturating_sub(ts) >> self.shift) as usize).min(MAX_AGE - 1)
  }

  #[inline(always)]
  fn class_id(age: u32) -> usize {
    if age == 0 {
      return AGE_CLASSES - 1;
    }
    let lz = age.leading_zeros() as usize;
    lz.saturating_sub(19).min(AGE_CLASSES - 1)
  }

  /// Get hit density components for efficient comparison
  /// 获取命中密度组件以进行高效比较
  /// Returns (bucket_density, size) to avoid division
  /// 返回 (bucket_density, size) 以避免除法
  #[inline(always)]
  fn density_components(&self, idx: usize) -> (f32, f32) {
    let m = unsafe { self.metas.get_unchecked(idx) };
    let age = self.age(m.ts);
    let cid = Self::class_id(m.last_age as u32 + m.prev_age as u32);
    let bucket_density = unsafe { self.buckets.get_unchecked(cid * MAX_AGE + age).density };
    (bucket_density, m.size as f32)
  }

  #[inline]
  fn evict(&mut self) {
    let n = self.metas.len();
    if n == 0 {
      return;
    }

    let samples = SAMPLES.min(n);
    let mut victim = self.rng.usize(0..n);

    // Get components: (bucket_density, size)
    let (mut min_b, mut min_s) = self.density_components(victim);

    // Compare via multiplication to avoid division
    // d1 < min_d  <=>  b/s < min_b/min_s  <=>  b * min_s < min_b * s
    // 通过乘法比较避免除法
    for _ in 1..samples {
      let idx = self.rng.usize(0..n);
      let (b, s) = self.density_components(idx);
      if b * min_s < min_b * s {
        min_b = b;
        min_s = s;
        victim = idx;
      }
    }

    // Remove key from map (cold access)
    // 从 Map 中移除 key（冷访问）
    let key = &unsafe { self.payloads.get_unchecked(victim) }.key;
    self.index.remove(key);
    self.rm_internal(victim);
  }

  #[inline]
  fn rm_internal(&mut self, idx: usize) {
    let last = self.metas.len() - 1;
    if idx != last {
      self.metas.swap(idx, last);
      self.payloads.swap(idx, last);
      // Update index for moved item
      // 更新被移动元素的索引
      let moved_key = &unsafe { self.payloads.get_unchecked(idx) }.key;
      unsafe {
        *self.index.get_mut(moved_key).unwrap_unchecked() = idx;
      }
    }

    // Safety: len > 0 guaranteed by caller
    // 安全性：调用者保证 len > 0
    let m = unsafe { self.metas.pop().unwrap_unchecked() };
    self.payloads.pop();
    self.stat_evict(&m);
    self.total -= m.size as usize;
  }

  #[inline]
  fn stat_evict(&mut self, m: &Meta) {
    let age = self.age(m.ts);
    let cid = Self::class_id(m.last_age as u32 + m.prev_age as u32);
    unsafe {
      self.buckets.get_unchecked_mut(cid * MAX_AGE + age).evicts += 1.0;
    }
  }

  #[cold]
  fn reconfig(&mut self) {
    self.last_cfg = self.ts;

    // Loop vectorization hint: flattened array allows compiler to vectorize easier
    for cid in 0..AGE_CLASSES {
      let offset = cid * MAX_AGE;
      let mut events = 0.0f32;
      let mut hits_sum = 0.0f32;
      let mut life = 0.0f32;

      // Backward dependency prevents simple SIMD, but pipelining helps
      for i in (0..MAX_AGE).rev() {
        unsafe {
          let b = self.buckets.get_unchecked_mut(offset + i);
          b.hits *= DECAY;
          b.evicts *= DECAY;
          hits_sum += b.hits;
          events += b.hits + b.evicts;
          life += events;
          // Use a small epsilon to avoid div by zero, faster than branch
          b.density = hits_sum / (life + 1e-9);
        }
      }
    }

    // Adapt age coarsening
    let n = self.metas.len();
    if n > 0 {
      let opt = (n as f32 / AGE_DIVISOR) as u32;
      let s = if opt <= 1 {
        0
      } else {
        opt.next_power_of_two().trailing_zeros()
      };
      self.shift = s.min(20);
    }
  }
}

impl<K: Hash + Eq + Clone, V> SizeLru<K, V> for Lhd<K, V> {
  #[inline(always)]
  fn get(&mut self, key: &K) -> Option<&V> {
    self.get(key)
  }

  #[inline(always)]
  fn set(&mut self, key: K, val: V, size: u32) {
    self.set(key, val, size);
  }

  #[inline(always)]
  fn rm(&mut self, key: &K) {
    self.rm(key);
  }
}
