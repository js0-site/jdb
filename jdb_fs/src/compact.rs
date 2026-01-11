//! Compact trait for log-structured data
//! 日志结构数据的压缩 trait

use crate::{consts::COMPACT_INTERVAL, item::Item};

/// Increment count flag
/// 增加计数标志
pub type IncrCount = bool;

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
pub trait Compact: Sized + Item + Default {
  /// Compact operation interval (operations per compaction)
  /// 压缩操作间隔（每次压缩的操作次数）
  const INTERVAL: usize = COMPACT_INTERVAL;

  /// Handle decoded data, return true if should increment compact count
  /// 处理解码的数据，如果需要增加压缩计数则返回 true
  fn on_head(&mut self, data: Self::Head) -> IncrCount;

  /// Iterate data for full rewrite
  /// 迭代数据用于完全重写
  fn rewrite(&self) -> impl Iterator<Item = &Self::Head>;
}
