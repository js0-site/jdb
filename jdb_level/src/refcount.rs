//! Reference count map for version tracking
//! 版本引用计数映射

use std::collections::HashMap;

/// Reference count map for version tracking
/// 版本引用计数映射
#[derive(Default)]
pub struct RefCountMap {
  /// Version -> count mapping
  /// 版本 -> 计数映射
  counts: HashMap<u64, u32>,
  /// Pending SSTable IDs to delete when refcount allows
  /// 待删除的 SSTable ID（等待引用计数允许）
  pending: Vec<(u64, u64)>, // (ver, sst_id)
}

impl RefCountMap {
  /// Create new empty RefCountMap
  /// 创建新的空 RefCountMap
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  /// Increment refcount for version
  /// 增加版本引用计数
  #[inline]
  pub fn inc(&mut self, ver: u64) {
    *self.counts.entry(ver).or_insert(0) += 1;
  }

  /// Decrement refcount for version, returns true if reached zero
  /// 减少版本引用计数，如果归零返回 true
  pub fn dec(&mut self, ver: u64) -> bool {
    if let Some(count) = self.counts.get_mut(&ver) {
      *count = count.saturating_sub(1);
      if *count == 0 {
        self.counts.remove(&ver);
        return true;
      }
    }
    false
  }

  /// Get refcount for version
  /// 获取版本引用计数
  #[inline]
  pub fn get(&self, ver: u64) -> u32 {
    self.counts.get(&ver).copied().unwrap_or(0)
  }

  /// Check if any version <= given ver has active refs
  /// 检查是否有 <= 给定版本的活跃引用
  #[inline]
  pub fn has_refs_before(&self, ver: u64) -> bool {
    self.counts.keys().any(|&v| v <= ver)
  }

  /// Add pending deletion
  /// 添加待删除项
  #[inline]
  pub fn add_pending(&mut self, ver: u64, sst_id: u64) {
    self.pending.push((ver, sst_id));
  }

  /// Drain deletions that are now safe (no refs to their version or earlier)
  /// 排出现在安全的删除项（没有对其版本或更早版本的引用）
  pub fn drain_safe(&mut self) -> Vec<u64> {
    let min_active = self.counts.keys().copied().min();
    let mut safe = Vec::new();
    self.pending.retain(|(ver, id)| match min_active {
      Some(min) if *ver >= min => true,
      _ => {
        safe.push(*id);
        false
      }
    });
    safe
  }
}
