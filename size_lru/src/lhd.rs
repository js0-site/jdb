//! LHD (Least Hit Density) cache for variable-sized data
//! LHD（最低命中密度）缓存，用于变长数据
//!
//! Based on: Beckmann et al. "LHD: Improving Cache Hit Rate by Maximizing Hit Density" (NSDI 2018)
//! 基于论文：Beckmann 等人的 LHD 算法
//!
//! Core idea: evict items with lowest expected_hits / size
//! 核心思想：淘汰 预期命中数/大小 最低的条目

use std::hash::Hash;

use fastrand::Rng;
use gxhash::{HashMap, HashMapExt};

use crate::SizeLru;

// Number of candidates to sample for eviction
// 淘汰时采样的候选者数量
const SAMPLES: usize = 32;

// Age classes for hit pattern classification
// 命中模式分类的年龄类别数
const AGE_CLASSES: usize = 16;

// Max age buckets for statistics
// 统计用的最大年龄桶数
const MAX_AGE: usize = 4096;

// EWMA decay factor
// EWMA 衰减因子
const DECAY: f32 = 0.9;

// Reconfigure interval (access count)
// 重配置间隔（访问次数）
const RECONFIG: u64 = 1 << 16;

// Class ID offset for leading_zeros calculation
// class_id 计算中 leading_zeros 的偏移量
// 32 - log2(MAX_AGE) - 1 = 32 - 12 - 1 = 19
const CLASS_LZ_OFF: usize = 19;

/// Entry metadata
/// 条目元数据
struct Entry<K, V> {
  key: K,
  val: V,
  ts: u64,
  size: u32,
  last_age: u16,
  prev_age: u16,
}

/// Age class statistics
/// 年龄类别统计
struct Class {
  hits: Vec<f32>,
  evicts: Vec<f32>,
  density: Vec<f32>,
}

impl Class {
  fn new() -> Self {
    // Init to GDSF-like: density ~ 1/age
    // 初始化为类 GDSF 分布
    let density = (0..MAX_AGE).map(|a| 1.0 / (a as f32 + 1.0)).collect();
    Self {
      hits: vec![0.0; MAX_AGE],
      evicts: vec![0.0; MAX_AGE],
      density,
    }
  }

  /// Apply EWMA decay
  /// 应用 EWMA 衰减
  fn decay(&mut self) {
    for h in &mut self.hits {
      *h *= DECAY;
    }
    for e in &mut self.evicts {
      *e *= DECAY;
    }
  }

  /// Recompute densities using conditional probability
  /// 使用条件概率重新计算密度
  fn recompute(&mut self) {
    let mut events = 0.0;
    let mut hits = 0.0;
    let mut life = 0.0;

    // Iterate backwards from MAX_AGE - 1 to 0
    // 从 MAX_AGE - 1 反向迭代到 0，涵盖所有桶
    for a in (0..MAX_AGE).rev() {
      // Safety: loop bounds 0..MAX_AGE are within Vec bounds
      // 安全性：循环边界在 Vec 范围内
      let (h, e, d) = unsafe {
        (
          *self.hits.get_unchecked(a),
          *self.evicts.get_unchecked(a),
          self.density.get_unchecked_mut(a),
        )
      };
      hits += h;
      events += h + e;
      life += events;

      // Avoid division by zero
      // 避免除以零
      *d = if life > 1e-9 { hits / life } else { 0.0 };
    }
  }
}

/// LHD cache with random sampling eviction
/// 带随机采样淘汰的 LHD 缓存
pub struct Lhd<K, V> {
  entries: Vec<Entry<K, V>>,
  index: HashMap<K, usize>,
  classes: Vec<Class>,
  total: usize,
  max: usize,
  ts: u64,
  shift: u32,
  next_cfg: u64,
  rng: Rng,
}

impl<K: Hash + Eq, V> Lhd<K, V> {
  /// Create with max memory in bytes
  /// 创建，指定最大内存（字节）
  pub fn new(max: usize) -> Self {
    Self {
      entries: Vec::new(),
      index: HashMap::new(),
      classes: (0..AGE_CLASSES).map(|_| Class::new()).collect(),
      total: 0,
      max: max.max(1),
      ts: 0,
      shift: 6,
      next_cfg: RECONFIG,
      rng: Rng::new(),
    }
  }

  /// Get value and update statistics
  /// 获取值并更新统计
  pub fn get(&mut self, key: &K) -> Option<&V> {
    self.ts = self.ts.wrapping_add(1);
    if self.ts >= self.next_cfg {
      self.reconfig();
    }

    let &idx = self.index.get(key)?;

    // Read entry metadata
    // 读取条目元数据
    // Safety: idx comes from map, guaranteed to be in entries
    // 安全性：idx 来自 map，保证在 entries 范围内
    let (old_ts, last, prev) = unsafe {
      let e = self.entries.get_unchecked(idx);
      (e.ts, e.last_age, e.prev_age)
    };

    let age = self.age(old_ts);
    let cid = Self::class_id(last as u32 + prev as u32);

    // Update entry
    // 更新条目
    // Safety: idx is valid
    // 安全性：idx 有效
    unsafe {
      let e = self.entries.get_unchecked_mut(idx);
      e.ts = self.ts;
      e.prev_age = last;
      e.last_age = age as u16;
    }

    // Update stats
    // 更新统计
    if cid < AGE_CLASSES {
      // Safety: cid < AGE_CLASSES, age < MAX_AGE
      // 安全性：cid < AGE_CLASSES, age < MAX_AGE
      unsafe {
        *self
          .classes
          .get_unchecked_mut(cid)
          .hits
          .get_unchecked_mut(age) += 1.0;
      }
    }

    // Safety: idx is valid
    // 安全性：idx 有效
    Some(&unsafe { self.entries.get_unchecked(idx) }.val)
  }

  /// Insert with size
  /// 插入并指定大小
  pub fn set(&mut self, key: K, val: V, size: u32)
  where
    K: Clone,
  {
    self.ts = self.ts.wrapping_add(1);
    if self.ts >= self.next_cfg {
      self.reconfig();
    }

    let sz = size as usize;

    // Update existing entry
    // 更新现有条目
    if let Some(&idx) = self.index.get(&key) {
      // Safety: idx from map is valid
      // 安全性：idx 来自 map，有效
      let e = unsafe { self.entries.get_unchecked_mut(idx) };
      self.total = self
        .total
        .saturating_sub(e.size as usize)
        .saturating_add(sz);
      e.val = val;
      e.size = size;
      e.ts = self.ts;
      return;
    }

    // Evict if necessary
    // 必要时淘汰
    while self.total.saturating_add(sz) > self.max && !self.entries.is_empty() {
      self.evict();
    }

    // Insert new entry
    // 插入新条目
    let idx = self.entries.len();
    self.index.insert(key.clone(), idx);
    self.entries.push(Entry {
      key,
      val,
      ts: self.ts,
      size,
      last_age: 0,
      prev_age: MAX_AGE as u16,
    });
    self.total = self.total.saturating_add(sz);
  }

  /// Remove by key
  /// 按键删除
  pub fn rm(&mut self, key: &K) {
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
    self.entries.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.entries.is_empty()
  }

  /// Calculate coarsened age
  /// 计算粗化年龄
  #[inline(always)]
  fn age(&self, ts: u64) -> usize {
    ((self.ts.saturating_sub(ts) >> self.shift) as usize).min(MAX_AGE - 1)
  }

  /// Map age to class (log scale)
  /// 年龄映射到类别（对数）
  #[inline(always)]
  fn class_id(age: u32) -> usize {
    if age == 0 {
      return AGE_CLASSES - 1;
    }
    // 32 - leading_zeros = significant bits
    // 32 - leading_zeros = 有效位数
    let lz = age.leading_zeros() as usize;
    lz.saturating_sub(CLASS_LZ_OFF).min(AGE_CLASSES - 1)
  }

  /// Get hit density for entry at index
  /// 获取指定索引条目的命中密度
  #[inline]
  fn density(&self, idx: usize) -> f32 {
    // Safety: Caller ensures idx is valid
    // 安全性：调用者保证 idx 有效
    let e = unsafe { self.entries.get_unchecked(idx) };
    let age = self.age(e.ts);
    let cid = Self::class_id(e.last_age as u32 + e.prev_age as u32);

    if cid < AGE_CLASSES {
      // Safety: cid and age verified by construction
      // 安全性：cid 和 age 由构造保证
      unsafe { *self.classes.get_unchecked(cid).density.get_unchecked(age) / e.size as f32 }
    } else {
      0.0
    }
  }

  /// Evict lowest density item via sampling
  /// 通过采样淘汰最低密度条目
  fn evict(&mut self) {
    let n = self.entries.len();
    if n == 0 {
      return;
    }

    let samples = SAMPLES.min(n);
    let mut victim = self.rng.usize(0..n);
    let mut min_d = self.density(victim);

    for _ in 1..samples {
      let idx = self.rng.usize(0..n);
      let d = self.density(idx);
      if d < min_d {
        min_d = d;
        victim = idx;
      }
    }

    // Remove from Map first, then entries
    // 先从 Map 移除，再移除条目
    // Safety: victim < n checked
    // 安全性：victim < n 已检查
    let key = &unsafe { self.entries.get_unchecked(victim) }.key;
    self.index.remove(key);
    self.rm_internal(victim);
  }

  /// Internal removal: swap with last, update index, pop
  /// 内部删除：与末尾交换、更新索引、弹出
  fn rm_internal(&mut self, idx: usize) {
    if let Some(entry) = self.swap_pop(idx) {
      self.stat_evict(&entry);
      self.total = self.total.saturating_sub(entry.size as usize);
    }
  }

  /// Swap item at idx with last, update index for moved item, pop
  /// 将 idx 处元素与末尾交换，更新移动元素索引，弹出
  fn swap_pop(&mut self, idx: usize) -> Option<Entry<K, V>> {
    let last = self.entries.len().checked_sub(1)?;

    if idx != last {
      self.entries.swap(idx, last);
      // Update index for moved item
      // 更新移动元素的索引
      // Safety: idx valid after swap
      // 安全性：交换后 idx 有效
      let moved_key = &unsafe { self.entries.get_unchecked(idx) }.key;
      if let Some(pos) = self.index.get_mut(moved_key) {
        *pos = idx;
      }
    }

    self.entries.pop()
  }

  /// Record eviction statistics
  /// 记录淘汰统计
  fn stat_evict(&mut self, e: &Entry<K, V>) {
    let age = self.age(e.ts);
    let cid = Self::class_id(e.last_age as u32 + e.prev_age as u32);
    if cid < AGE_CLASSES {
      unsafe {
        *self
          .classes
          .get_unchecked_mut(cid)
          .evicts
          .get_unchecked_mut(age) += 1.0;
      }
    }
  }

  #[cold]
  fn reconfig(&mut self) {
    self.next_cfg = self.ts.wrapping_add(RECONFIG);

    for c in &mut self.classes {
      c.decay();
      c.recompute();
    }

    // Adapt age coarsening
    // 调整年龄粗化
    let n = self.entries.len();
    if n > 0 {
      let opt = (n as f32 / (0.01 * MAX_AGE as f32)).max(1.0);
      let mut s = 0u32;
      while (1u32 << s) < opt as u32 {
        s += 1;
      }
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
