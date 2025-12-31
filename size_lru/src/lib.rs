//! Size-aware cache library with multiple implementations
//! 大小感知缓存库，支持多种实现
//!
//! This library provides a common trait for size-aware caches with different eviction strategies.
//! 本库为大小感知缓存提供通用 trait，支持不同的淘汰策略。
//!
//! # Features
//! 特性
//!
//! - `lhd`: LHD (Least Hit Density) cache implementation
//! - `no`: `NoCache` - zero overhead no-op

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{borrow::Borrow, hash::Hash};

/// Callback on entry removal/eviction
/// Called before actual removal or eviction, so cache.get(key) still works
/// 条目删除/淘汰时的回调
/// 在实际删除或淘汰前调用，此时 cache.get(key) 仍可用
pub trait OnRm<K, C> {
  fn call(&mut self, key: &K, cache: &mut C);
}

/// No-op callback (zero overhead)
/// 空回调（零开销）
pub struct NoOnRm;

impl<K, C> OnRm<K, C> for NoOnRm {
  #[inline(always)]
  fn call(&mut self, _: &K, _: &mut C) {}
}

/// Size-aware cache trait
/// 大小感知缓存 Trait
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
  /// 查看值但不更新统计
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
  /// 检查缓存是否为空
  fn is_empty(&self) -> bool;
  /// Get entry count
  /// 获取条目数量
  fn len(&self) -> usize;
}

#[cfg(feature = "lhd")]
mod lhd;

#[cfg(feature = "no")]
mod no;

#[cfg(feature = "lhd")]
pub use lhd::Lhd;
#[cfg(feature = "no")]
pub use no::NoCache;
