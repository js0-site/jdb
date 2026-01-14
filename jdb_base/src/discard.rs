//! Discard handler for merge stream
//! 合并流的丢弃处理器

use std::{fmt::Debug, future::Future};

use crate::Pos;

/// Discard handler trait
/// 丢弃处理器 trait
pub trait Discard: 'static {
  type Error: Send + Debug;

  /// Called when a key-value is discarded (old version or tombstone at bottom)
  /// 当 key-value 被丢弃时调用（老版本或最底层墓碑）
  fn discard(&mut self, key: &[u8], pos: &Pos);

  /// Flush discard buffer to storage
  /// 将丢弃缓冲区刷入存储
  fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>>;
}
