//! SsTable - Async query interface for SSTable
//! 异步表 - SSTable 的异步查询接口

use std::{future::Future, ops::Bound};

use futures_core::Stream;

use crate::{Kv, Pos};

/// Async query trait for SSTable (block-level lazy loading, O(1) memory)
/// SSTable 的异步查询 trait（块级惰性加载，O(1) 内存）
pub trait SsTable {
  /// Stream type for range queries
  /// 范围查询的流类型
  type RangeStream<'a>: Stream<Item = Kv> + Unpin
  where
    Self: 'a;

  /// Reverse stream type for range queries
  /// 反向范围查询的流类型
  type RevStream<'a>: Stream<Item = Kv> + Unpin
  where
    Self: 'a;

  /// Get entry by key (async)
  /// No Send bound strictly required for single-threaded runtimes (e.g. compio)
  /// 按键获取条目（异步）
  fn get(&mut self, key: &[u8]) -> impl Future<Output = Option<Pos>>;

  /// Forward range query (ascending order)
  /// 正向范围查询（升序）
  fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RangeStream<'_>;

  /// Reverse range query (descending order)
  /// 反向范围查询（降序）
  fn rev_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RevStream<'_>;

  /// Forward iterate all entries
  /// 正向迭代所有条目
  #[inline]
  fn iter(&mut self) -> Self::RangeStream<'_> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Reverse iterate all entries
  /// 反向迭代所有条目
  #[inline]
  fn rev_iter(&mut self) -> Self::RevStream<'_> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }
}
