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
//! - `no`: NoCache - zero overhead no-op

#![cfg_attr(docsrs, feature(doc_cfg))]

/// Size-aware cache trait
/// 大小感知缓存 Trait
pub trait SizeLru<K, V> {
  /// Get value
  /// 获取值
  fn get(&mut self, key: &K) -> Option<&V>;

  /// Set value with size
  /// 设置值及其大小
  fn set(&mut self, key: K, val: V, size: u32);

  /// Remove by key
  /// 按键删除
  fn rm(&mut self, key: &K);
}

#[cfg(feature = "lhd")]
mod lhd;

#[cfg(feature = "no")]
mod no;

#[cfg(feature = "lhd")]
pub use lhd::Lhd;
#[cfg(feature = "no")]
pub use no::NoCache;
