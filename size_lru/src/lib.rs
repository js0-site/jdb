//! Size-aware cache library with multiple implementations
//!
//! This library provides a common trait for size-aware caches with different eviction strategies.
//!
//! # Features
//!
//! - `lhd`: LHD (Least Hit Density) cache implementation
//! - `no`: `NoCache` - zero overhead no-op
//!
//! 大小感知缓存库，支持多种实现
//!
//! 本库为大小感知缓存提供通用 trait，支持不同的淘汰策略。
//!
//! # 特性

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{borrow::Borrow, hash::Hash};

/// Callback on entry removal/eviction
/// Called before actual removal or eviction, so cache.get(key) still works
///
/// # Design Rationale
///
/// Why callback only passes key, not value?
/// - Many use cases only need key (logging, counting, notifying external systems)
/// - If value not needed, avoids one memory access overhead
/// - When value needed, call `cache.get(key)` to retrieve it
///
/// # Safety Warning
///
/// NEVER call `rm` or `set` inside callback - causes undefined behavior!
///
/// Reasons:
///
/// 1. Reentrancy: callback runs inside evict/rm while cache is in intermediate state
///    - index may have removed key, but metas/payloads not yet cleaned up
///    - calling set may trigger new eviction, causing recursive eviction
///
/// 2. Iterator invalidation: rm_idx uses swap_remove, moving last element to deleted position
///    - removing other entries in callback may corrupt ongoing swap operation
///    - leads to index pointing to wrong position or dangling references
///
/// 3. Borrow conflict: callback holds &mut cache, internal ops also need &mut self
///    - Rust borrow checker bypassed via unsafe, but data race actually exists
///
/// 条目删除/淘汰时的回调
/// 在实际删除或淘汰前调用，此时 cache.get(key) 仍可用
///
/// # 设计原因
///
/// 为什么回调只传 key 而不传 value？
/// - 很多场景只需要 key（如日志、计数、通知外部系统）
/// - 若不需要 value，可避免一次内存访问开销
/// - 需要 value 时，调用 cache.get(key) 即可获取
///
/// # 安全警告
///
/// 禁止在回调中调用 `rm` 或 `set`，会导致未定义行为！
///
/// 原因：
///
/// 1. 重入问题：回调在 evict/rm 内部执行，此时缓存正处于中间状态
///    - index 可能已删除 key，但 metas/payloads 尚未清理
///    - 调用 set 可能触发新的 evict，形成递归淘汰
///
/// 2. 迭代器失效：rm_idx 使用 swap_remove，会移动最后一个元素到被删位置
///    - 若回调中删除其他条目，可能破坏正在进行的 swap 操作
///    - 导致 index 指向错误位置或悬垂引用
///
/// 3. 借用冲突：回调持有 &mut cache，内部操作也需要 &mut self
///    - Rust 借用检查器被 unsafe 绕过，但实际存在数据竞争
pub trait OnRm<K, C> {
  fn call(&mut self, key: &K, cache: &mut C);
}

/// No-op callback (zero overhead)
pub struct NoOnRm;

//
/// 空回调（零开销）

impl<K, C> OnRm<K, C> for NoOnRm {
  #[inline(always)]
  fn call(&mut self, _: &K, _: &mut C) {}
}

/// Size-aware cache trait
pub trait SizeLru<K, V>: Sized {
  type WithRm<Rm>;

  fn new(max: usize) -> Self::WithRm<NoOnRm> {
    Self::with_on_rm(max, NoOnRm)
  }
  fn with_on_rm<Rm>(max: usize, on_rm: Rm) -> Self::WithRm<Rm>;
  fn get<Q>(&mut self, key: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized;
  /// Peek value without updating stats
  fn peek<Q>(&self, key: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized;
  fn set(&mut self, key: K, val: V, size: u32);
  fn rm<Q>(&mut self, key: &Q)
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized;
  /// Check if cache is empty
  fn is_empty(&self) -> bool;
  /// Get entry count
  fn len(&self) -> usize;
}

//
/// 大小感知缓存 Trait
/// 查看值但不更新统计
/// 检查缓存是否为空
/// 获取条目数量

#[cfg(feature = "lhd")]
mod lhd;

#[cfg(feature = "no")]
mod no;

#[cfg(feature = "lhd")]
pub use lhd::Lhd;
#[cfg(feature = "no")]
pub use no::NoCache;
