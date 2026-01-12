use std::fmt::Debug;

use super::Meta;
use crate::Pos;

/// Flush memtable to SST
/// 将内存表刷到 SST
pub trait Flush: Send + 'static {
  type Error: Send + Debug;

  /// Flush memtable to disk
  /// 将内存表刷到磁盘
  fn flush<'a>(
    &mut self,
    iter: impl Iterator<Item = (&'a [u8], Pos)>,
  ) -> impl Future<Output = Result<Meta, Self::Error>>;
}
