use std::{fmt::Debug, future::Future};

use crate::{Pos, ckp::Meta};

pub type Kv<'a> = (&'a [u8], Pos);

/// Flush memtable to SST
/// 将内存表刷到 SST
pub trait MemToSst: Send + 'static {
  type Error: Send + Debug;

  /// Flush memtable to disk
  /// 将内存表刷到磁盘
  fn write<'a>(
    &self,
    iter: impl Iterator<Item = Kv<'a>>,
  ) -> impl Future<Output = Result<Meta, Self::Error>>;

  // 确保移除mem和添加sst无缝操作（之间没有await）
  fn push(&mut self, meta: Meta);
}
