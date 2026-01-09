//! Discard handler for merge stream
//! 合并流的丢弃处理器

use jdb_base::Pos;

/// Discard handler trait
/// 丢弃处理器 trait
pub trait OnDiscard {
  /// Called when a key-value is discarded (old version or tombstone at bottom)
  /// 当 key-value 被丢弃时调用（老版本或最底层墓碑）
  fn discard(&mut self, key: &[u8], pos: &Pos);
}

/// No-op discard handler (for read-only queries)
/// 空操作丢弃处理器（用于只读查询）
#[derive(Default, Clone, Copy)]
pub struct NoDiscard;

impl OnDiscard for NoDiscard {
  #[inline]
  fn discard(&mut self, _key: &[u8], _pos: &Pos) {}
}
