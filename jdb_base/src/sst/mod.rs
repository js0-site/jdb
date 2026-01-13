//! SSTable - Async query interface for SSTable
//! 异步表 - SSTable 的异步查询接口

pub mod ckp;
pub mod ckp_op;
mod meta;
pub mod meta_li;
mod query;

use std::{fmt::Debug, future::Future};

pub use ckp_op::CkpOp;
pub use meta::Meta;
pub use meta_li::MetaLi;
pub use query::Query;

use crate::Pos;

pub type Kv<'a> = (&'a [u8], Pos);

/// Flush memtable to SST
/// 将内存表刷到 SST
pub trait Sst: Send + 'static {
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
