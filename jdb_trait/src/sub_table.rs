use std::{fmt::Debug, future::Future};

use futures_core::Stream;

use crate::{AsyncRow, Id, Order, Query, Row, SubTableKey};

/// 子表 trait SubTable trait
pub trait SubTable: Send + Sync {
  type Error: Debug + Send + Sync;
  type AsyncRow: AsyncRow;
  type Stream: Stream<Item = Result<(Id, Self::AsyncRow), Self::Error>> + Send;

  fn put(&self, data: &[Row]) -> impl Future<Output = Result<Vec<Id>, Self::Error>> + Send;

  /// 根据 ID 获取单个记录 Get single record by ID
  fn get(
    &self,
    id: Id,
  ) -> impl Future<Output = Result<Option<(Id, Self::AsyncRow)>, Self::Error>> + Send;

  /// 索引查询 Index query
  fn select(&self, q: &Query) -> impl Future<Output = Self::Stream> + Send;

  fn scan(&self, begin_id: u64, order: Order) -> impl Future<Output = Self::Stream> + Send;

  fn history(&self, id: Id, offset: usize) -> impl Future<Output = Self::Stream> + Send;

  fn rm(&self, q: &Query) -> impl Future<Output = Result<u64, Self::Error>> + Send;

  /// 获取子表键 Get sub-table key
  fn key(&self) -> &SubTableKey;

  /// 获取或插入记录，如果不存在则使用提供的函数创建新记录
  /// Get or insert record, create new record using provided function if not exists
  fn get_or_insert_with<F>(
    &self,
    query: &Query,
    f: F,
  ) -> impl Future<Output = Result<(Id, Self::AsyncRow), Self::Error>> + Send
  where
    F: FnOnce() -> Row + Send;
}
