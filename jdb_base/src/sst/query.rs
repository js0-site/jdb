//! SSTable query interface - Async query operations for SSTable
//! SSTable 查询接口 - SSTable 的异步查询操作

use core::ops::RangeBounds;
use std::borrow::Borrow;

use futures_core::Stream;

use crate::Pos;

/// Async query interface for SSTable
/// SSTable 的异步查询接口
pub trait Query {
  /// Key type returned by stream
  /// 流返回的键类型
  type Key<'a>
  where
    Self: 'a;

  /// Forward stream type
  /// 正向流类型
  type Stream<'a>: Stream<Item = (Self::Key<'a>, Pos)>
  where
    Self: 'a;

  /// Reverse stream type
  /// 反向流类型
  type RevStream<'a>: Stream<Item = (Self::Key<'a>, Pos)>
  where
    Self: 'a;

  /// Error type
  /// 错误类型
  type Error: std::fmt::Debug + Send;

  /// Async get value by key
  /// 异步根据键获取值
  fn get(
    &self,
    key: impl Borrow<[u8]>,
  ) -> impl std::future::Future<Output = Result<Option<Pos>, Self::Error>>;

  /// Forward stream over all entries
  /// 正向遍历所有条目的流
  fn iter(&self) -> Self::Stream<'_>;

  /// Reverse stream over all entries
  /// 反向遍历所有条目的流
  fn rev_iter(&self) -> Self::RevStream<'_>;

  /// Forward stream over entries in range
  /// 正向遍历范围内条目的流
  fn range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::Stream<'_>;

  /// Reverse stream over entries in range
  /// 反向遍历范围内条目的流
  fn rev_range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::RevStream<'_>;
}
