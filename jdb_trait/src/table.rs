use std::{fmt::Debug, future::Future};

use futures_core::Stream;

use crate::{AsyncItem, AsyncRow, Id, Order, Query, Row, SubTable, SubTableKey, schema::Schema};

pub trait Table: Sized + Send + Sync {
  type Error: Debug + Send + Sync;
  type SubTable: SubTable;
  type AsyncRow: AsyncRow;
  type Stream: Stream<Item = Result<AsyncItem<Self::AsyncRow>, Self::Error>> + Send;

  /// 获取当前 Schema Get current schema
  fn schema(&self) -> impl Future<Output = Schema> + Send;

  fn put(
    &self,
    key: &SubTableKey,
    data: &[Row],
  ) -> impl Future<Output = Result<Vec<Id>, Self::Error>> + Send;

  fn get(
    &self,
    key: &SubTableKey,
    id: Id,
  ) -> impl Future<Output = Result<Option<AsyncItem<Self::AsyncRow>>, Self::Error>> + Send;

  fn get_or_insert_with<F>(
    &self,
    key: &SubTableKey,
    query: &Query,
    f: F,
  ) -> impl Future<Output = Result<AsyncItem<Self::AsyncRow>, Self::Error>> + Send
  where
    F: FnOnce() -> Row + Send;

  fn compact(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  fn select(&self, q: &Query) -> impl Future<Output = Self::Stream> + Send;

  fn scan(&self, begin_id: u64, order: Order) -> impl Future<Output = Self::Stream> + Send;

  fn history(
    &self,
    key: &SubTableKey,
    id: Id,
    offset: usize,
  ) -> impl Future<Output = Self::Stream> + Send;

  fn rm(&self, q: &Query) -> impl Future<Output = Result<u64, Self::Error>> + Send;

  fn sub_exists(&self, key: &SubTableKey) -> impl Future<Output = bool> + Send;

  fn sub(&self, key: &SubTableKey) -> impl Future<Output = Option<Self::SubTable>> + Send;
}
