//! Garbage Collection for Page and VLog
//! 页和值日志的垃圾回收

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;

use std::{collections::BTreeMap, ops::BitOrAssign, path::Path};

pub use error::{Error, Result};
use jdb_page::PageStore;
use jdb_trait::{TableId, ValRef};
use roaring::RoaringBitmap;

/// Segment size for chunked GC (1M pages) / 分段 GC 的段大小
const SEGMENT_SIZE: u32 = 1024 * 1024;

/// Segment bitmap size in bytes (128KB) / 段位图大小
const SEGMENT_BYTES: usize = SEGMENT_SIZE as usize / 8;

/// Segment bitmap for O(1) space GC / O(1) 空间的段位图
pub struct SegmentBitmap {
  bits: Box<[u8; SEGMENT_BYTES]>,
  base: u32,
}

impl SegmentBitmap {
  /// Create for segment / 为段创建
  pub fn new(segment: u32) -> Self {
    Self {
      bits: Box::new([0u8; SEGMENT_BYTES]),
      base: segment * SEGMENT_SIZE,
    }
  }

  /// Mark page as reachable / 标记页为可达
  #[inline]
  pub fn mark(&mut self, page_id: u64) {
    if page_id < self.base as u64 || page_id >= (self.base + SEGMENT_SIZE) as u64 {
      return;
    }
    let idx = (page_id - self.base as u64) as usize;
    self.bits[idx / 8] |= 1 << (idx % 8);
  }

  /// Check if page is reachable / 检查页是否可达
  #[inline]
  pub fn is_marked(&self, page_id: u64) -> bool {
    if page_id < self.base as u64 || page_id >= (self.base + SEGMENT_SIZE) as u64 {
      return false;
    }
    let idx = (page_id - self.base as u64) as usize;
    (self.bits[idx / 8] & (1 << (idx % 8))) != 0
  }

  /// Sweep segment, return freed count / 清扫段，返回释放数
  pub fn sweep(&self, store: &mut PageStore) -> usize {
    let end = (self.base + SEGMENT_SIZE).min(store.page_count() as u32);
    let start = self.base.max(1); // skip page 0
    let mut count = 0;

    for page_id in start..end {
      if !self.is_marked(page_id as u64) {
        store.free(page_id as u64);
        count += 1;
      }
    }
    count
  }

  /// Get segment range / 获取段范围
  pub fn range(&self) -> (u32, u32) {
    (self.base, self.base + SEGMENT_SIZE)
  }

  /// Memory size (constant) / 内存大小（常量）
  pub const fn mem_size() -> usize {
    SEGMENT_BYTES
  }
}

/// Page GC with segmented sweep / 分段清扫的页 GC
pub struct PageGc {
  reachable: RoaringBitmap,
}

impl PageGc {
  pub fn new() -> Self {
    Self {
      reachable: RoaringBitmap::new(),
    }
  }

  /// Mark page as reachable / 标记页为可达
  pub fn mark(&mut self, page_id: u64) {
    if page_id <= u32::MAX as u64 {
      self.reachable.insert(page_id as u32);
    }
  }

  /// Mark multiple pages / 标记多个页
  pub fn mark_all(&mut self, page_ids: impl IntoIterator<Item = u64>) {
    for id in page_ids {
      self.mark(id);
    }
  }

  /// Check if page is marked / 检查页是否已标记
  pub fn is_marked(&self, page_id: u64) -> bool {
    if page_id > u32::MAX as u64 {
      return false;
    }
    self.reachable.contains(page_id as u32)
  }

  /// Sweep one segment / 清扫一个段
  pub fn sweep_segment(&self, store: &mut PageStore, segment: u32) -> usize {
    let start = segment * SEGMENT_SIZE;
    let end = start
      .saturating_add(SEGMENT_SIZE)
      .min(store.page_count() as u32);
    let mut count = 0;

    for page_id in start.max(1)..end {
      if !self.reachable.contains(page_id) {
        store.free(page_id as u64);
        count += 1;
      }
    }
    count
  }

  /// Get segment count / 获取段数
  pub fn segment_count(total: u64) -> u32 {
    let total = total.min(u32::MAX as u64) as u32;
    total.div_ceil(SEGMENT_SIZE)
  }

  /// Sweep all / 清扫全部
  pub fn sweep(&self, store: &mut PageStore) -> usize {
    let segments = Self::segment_count(store.page_count());
    (0..segments).map(|s| self.sweep_segment(store, s)).sum()
  }

  /// Get stats / 获取统计
  pub fn stats(&self, total: u64) -> GcStats {
    let reachable = self.reachable.len();
    GcStats {
      total,
      reachable,
      garbage: total.saturating_sub(reachable).saturating_sub(1),
    }
  }

  /// Memory usage / 内存使用
  pub fn mem_size(&self) -> usize {
    self.reachable.serialized_size()
  }
}

impl Default for PageGc {
  fn default() -> Self {
    Self::new()
  }
}

/// VLog GC - collect old values / VLog GC - 回收旧值
pub struct VlogGc {
  live_files: RoaringBitmap,
}

impl VlogGc {
  pub fn new() -> Self {
    Self {
      live_files: RoaringBitmap::new(),
    }
  }

  /// Mark ValRef as live / 标记 ValRef 为存活
  pub fn mark(&mut self, vref: &ValRef) {
    if vref.file_id <= u32::MAX as u64 {
      self.live_files.insert(vref.file_id as u32);
    }
  }

  /// Mark multiple refs / 标记多个引用
  pub fn mark_all(&mut self, vrefs: impl IntoIterator<Item = ValRef>) {
    for vref in vrefs {
      self.mark(&vref);
    }
  }

  /// Check if file can be deleted / 检查文件是否可删除
  pub fn can_delete_file(&self, file_id: u64) -> bool {
    if file_id > u32::MAX as u64 {
      return false;
    }
    !self.live_files.contains(file_id as u32)
  }

  /// Get deletable files / 获取可删除文件
  pub fn deletable_files(&self, all_files: &[u64]) -> Vec<u64> {
    all_files
      .iter()
      .filter(|&&fid| self.can_delete_file(fid))
      .copied()
      .collect()
  }

  /// Delete old vlog files / 删除旧 vlog 文件
  pub fn delete_files(dir: impl AsRef<Path>, file_ids: &[u64]) -> Result<usize> {
    let dir = dir.as_ref();
    let mut count = 0;
    for &fid in file_ids {
      let path = dir.join(format!("{fid:08}.vlog"));
      if path.exists() {
        std::fs::remove_file(&path)?;
        count += 1;
      }
    }
    Ok(count)
  }

  /// Get live file count / 获取存活文件数
  pub fn live_count(&self) -> usize {
    self.live_files.len() as usize
  }

  /// Get live files / 获取存活文件
  pub fn live_files(&self) -> Vec<u64> {
    self.live_files.iter().map(|id| id as u64).collect()
  }

  /// Memory usage in bytes / 内存使用（字节）
  pub fn mem_size(&self) -> usize {
    self.live_files.serialized_size()
  }
}

impl Default for VlogGc {
  fn default() -> Self {
    Self::new()
  }
}

/// Retention policy for a fork / Fork 的保留策略
#[derive(Debug, Clone, Copy)]
pub struct RetentionPolicy {
  /// Max history versions (None = unlimited) / 最大历史版本数
  pub max_history: Option<usize>,
  /// Max age in milliseconds (None = unlimited) / 最大保留时间（毫秒）
  pub max_age_ms: Option<u64>,
  /// Keep all history / 保留全部历史
  pub keep_all: bool,
}

impl RetentionPolicy {
  /// Keep all history / 保留全部
  pub const fn keep_all() -> Self {
    Self {
      max_history: None,
      max_age_ms: None,
      keep_all: true,
    }
  }

  /// Keep only current value / 只保留当前值
  pub const fn current_only() -> Self {
    Self {
      max_history: Some(1),
      max_age_ms: None,
      keep_all: false,
    }
  }

  /// Keep N versions / 保留 N 个版本
  pub const fn versions(n: usize) -> Self {
    Self {
      max_history: Some(n),
      max_age_ms: None,
      keep_all: false,
    }
  }

  /// Keep for duration / 保留指定时长
  pub const fn duration_ms(ms: u64) -> Self {
    Self {
      max_history: None,
      max_age_ms: Some(ms),
      keep_all: false,
    }
  }

  /// Check if should keep version / 检查是否保留版本
  #[inline]
  pub fn should_keep(&self, version_idx: usize, age_ms: Option<u64>) -> bool {
    if self.keep_all {
      return true;
    }
    if let Some(max) = self.max_history
      && version_idx >= max
    {
      return false;
    }
    if let Some(max_age) = self.max_age_ms
      && let Some(age) = age_ms
      && age > max_age
    {
      return false;
    }
    true
  }
}

impl Default for RetentionPolicy {
  fn default() -> Self {
    Self::current_only()
  }
}

/// Fork-aware VLog GC / 感知 Fork 的 VLog GC
pub struct ForkAwareGc {
  /// Per-fork policies / 每个 fork 的策略
  policies: BTreeMap<TableId, RetentionPolicy>,
  /// Default policy / 默认策略
  default_policy: RetentionPolicy,
  /// Live records per file: file_id -> RoaringBitmap of (offset / 4096)
  /// 每文件存活记录
  live_per_file: BTreeMap<u64, RoaringBitmap>,
  /// Max files to cache / 最大缓存文件数
  max_cached_files: usize,
}

impl ForkAwareGc {
  pub fn new() -> Self {
    Self {
      policies: BTreeMap::new(),
      default_policy: RetentionPolicy::current_only(),
      live_per_file: BTreeMap::new(),
      max_cached_files: 64,
    }
  }

  /// Set default policy / 设置默认策略
  pub fn set_default_policy(&mut self, policy: RetentionPolicy) {
    self.default_policy = policy;
  }

  /// Set policy for fork / 设置 fork 策略
  pub fn set_policy(&mut self, fork_id: TableId, policy: RetentionPolicy) {
    self.policies.insert(fork_id, policy);
  }

  /// Get policy for fork / 获取 fork 策略
  pub fn policy(&self, fork_id: TableId) -> RetentionPolicy {
    self
      .policies
      .get(&fork_id)
      .copied()
      .unwrap_or(self.default_policy)
  }

  /// Remove policy for fork / 移除 fork 策略
  pub fn remove_policy(&mut self, fork_id: TableId) {
    self.policies.remove(&fork_id);
  }

  /// Mark ValRef as live / 标记 ValRef 为存活
  pub fn mark(&mut self, vref: &ValRef) {
    let offset_idx = (vref.real_offset() / 4096) as u32;
    self
      .live_per_file
      .entry(vref.file_id)
      .or_default()
      .insert(offset_idx);
    self.evict_if_needed();
  }

  /// Mark history chain with policy / 根据策略标记历史链
  /// Returns number of versions marked / 返回标记的版本数
  pub fn mark_history_with_policy(
    &mut self,
    history: &[ValRef],
    fork_id: TableId,
    now_ms: Option<u64>,
    timestamps: Option<&[u64]>,
  ) -> usize {
    let policy = self.policy(fork_id);
    let mut marked = 0;

    for (idx, vref) in history.iter().enumerate() {
      let age_ms = match (now_ms, timestamps) {
        (Some(now), Some(ts)) if idx < ts.len() => Some(now.saturating_sub(ts[idx])),
        _ => None,
      };

      if policy.should_keep(idx, age_ms) {
        self.mark(vref);
        marked += 1;
      } else {
        break; // 后续版本更旧，无需继续 / Older versions, stop
      }
    }
    marked
  }

  /// Merge live refs from multiple forks / 合并多个 fork 的存活引用
  /// Any ref needed by any fork is kept / 任一 fork 需要的都保留
  pub fn merge(&mut self, other: &ForkAwareGc) {
    for (&file_id, bitmap) in &other.live_per_file {
      self
        .live_per_file
        .entry(file_id)
        .or_default()
        .bitor_assign(bitmap);
    }
    self.evict_if_needed();
  }

  /// Check if record is live / 检查记录是否存活
  pub fn is_live(&self, file_id: u64, offset: u64) -> bool {
    let offset_idx = (offset / 4096) as u32;
    self
      .live_per_file
      .get(&file_id)
      .is_some_and(|bm| bm.contains(offset_idx))
  }

  /// Get live offsets in file / 获取文件中的存活偏移
  pub fn live_in_file(&self, file_id: u64) -> Vec<u64> {
    self
      .live_per_file
      .get(&file_id)
      .map(|bm| bm.iter().map(|idx| idx as u64 * 4096).collect())
      .unwrap_or_default()
  }

  /// Get files that can be deleted (no live records) / 获取可删除文件
  pub fn deletable_files(&self, all_files: &[u64]) -> Vec<u64> {
    all_files
      .iter()
      .filter(|&&fid| !self.live_per_file.contains_key(&fid))
      .copied()
      .collect()
  }

  /// Get files with garbage ratio / 获取有垃圾的文件及比例
  pub fn files_with_garbage(&self, file_stats: &[(u64, u64)]) -> Vec<(u64, f64)> {
    // file_stats: [(file_id, total_records)]
    let mut result = Vec::new();
    for &(fid, total) in file_stats {
      let live = self.live_per_file.get(&fid).map(|bm| bm.len()).unwrap_or(0);
      if live < total {
        let ratio = 1.0 - (live as f64 / total as f64);
        result.push((fid, ratio));
      }
    }
    result
  }

  /// Delete old vlog files / 删除旧 vlog 文件
  pub fn delete_files(dir: impl AsRef<Path>, file_ids: &[u64]) -> Result<usize> {
    let dir = dir.as_ref();
    let mut count = 0;
    for &fid in file_ids {
      let path = dir.join(format!("{fid:08}.vlog"));
      if path.exists() {
        std::fs::remove_file(&path)?;
        count += 1;
      }
    }
    Ok(count)
  }

  /// Get live file count / 获取存活文件数
  pub fn live_file_count(&self) -> usize {
    self.live_per_file.len()
  }

  /// Memory usage / 内存使用
  pub fn mem_size(&self) -> usize {
    self
      .live_per_file
      .values()
      .map(|bm| bm.serialized_size())
      .sum()
  }

  /// Clear all / 清空
  pub fn clear(&mut self) {
    self.live_per_file.clear();
  }

  /// Evict oldest files if over limit / 超限时淘汰最旧文件
  fn evict_if_needed(&mut self) {
    while self.live_per_file.len() > self.max_cached_files {
      if let Some(&oldest) = self.live_per_file.keys().next() {
        self.live_per_file.remove(&oldest);
      } else {
        break;
      }
    }
  }
}

impl Default for ForkAwareGc {
  fn default() -> Self {
    Self::new()
  }
}

/// Record-level VLog GC with compaction / 记录级 VLog GC（带压缩）
pub struct VlogCompactor {
  /// Live records: (file_id, offset) / 存活记录
  live: std::collections::BTreeSet<(u64, u64)>,
}

impl VlogCompactor {
  pub fn new() -> Self {
    Self {
      live: std::collections::BTreeSet::new(),
    }
  }

  /// Mark record as live / 标记记录为存活
  pub fn mark(&mut self, vref: &ValRef) {
    self.live.insert((vref.file_id, vref.real_offset()));
  }

  /// Mark with history chain / 标记含历史链
  pub fn mark_with_history(&mut self, vrefs: impl IntoIterator<Item = ValRef>) {
    for vref in vrefs {
      self.mark(&vref);
    }
  }

  /// Check if record is live / 检查记录是否存活
  pub fn is_live(&self, file_id: u64, offset: u64) -> bool {
    self.live.contains(&(file_id, offset))
  }

  /// Get live records in file / 获取文件中的存活记录
  pub fn live_in_file(&self, file_id: u64) -> Vec<u64> {
    self
      .live
      .range((file_id, 0)..=(file_id, u64::MAX))
      .map(|(_, off)| *off)
      .collect()
  }

  /// Get files with garbage / 获取有垃圾的文件
  pub fn files_with_garbage(&self, all_files: &[(u64, u64)]) -> Vec<(u64, f64)> {
    // all_files: [(file_id, record_count)]
    let mut result = Vec::new();
    for &(fid, total) in all_files {
      let live = self.live.range((fid, 0)..=(fid, u64::MAX)).count() as u64;
      if live < total {
        let ratio = 1.0 - (live as f64 / total as f64);
        result.push((fid, ratio));
      }
    }
    result
  }

  /// Get live count / 获取存活数
  pub fn live_count(&self) -> usize {
    self.live.len()
  }

  /// Clear / 清空
  pub fn clear(&mut self) {
    self.live.clear();
  }
}

impl Default for VlogCompactor {
  fn default() -> Self {
    Self::new()
  }
}

/// GC statistics / GC 统计
#[derive(Debug, Clone, Copy)]
pub struct GcStats {
  pub total: u64,
  pub reachable: u64,
  pub garbage: u64,
}

impl GcStats {
  /// Garbage ratio / 垃圾比例
  pub fn ratio(&self) -> f64 {
    if self.total == 0 {
      0.0
    } else {
      self.garbage as f64 / self.total as f64
    }
  }
}
