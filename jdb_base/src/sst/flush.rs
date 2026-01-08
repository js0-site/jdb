use std::fmt::Debug;

use oneshot::Receiver;

use super::Meta;
use crate::Pos;

/// Flush memtable to SST
/// 将内存表刷到 SST
pub trait Flush: Send + 'static {
  type Error: Send + Debug;

  /// Flush memtable to disk
  /// 将内存表刷到磁盘
  fn flush<'a, I>(&mut self, id: u64, iter: I) -> Receiver<Result<Meta, Self::Error>>
  where
    I: Iterator<Item = (&'a Box<[u8]>, &'a Pos)>;
}

/// Callback after flush completes
/// 刷盘完成回调
pub trait OnFlush: 'static {
  fn on_flush(&mut self, meta: Meta);
}
