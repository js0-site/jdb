#![cfg_attr(docsrs, feature(doc_cfg))]

use std::future::Future;

use bytes::Bytes;
use futures_core::Stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
  Asc,
  Desc,
}

pub type Kv = (Bytes, Bytes);
pub type KvLi = Vec<Kv>;
pub type Rev = u64;
pub type TableId = u64;

const TOMBSTONE_FLAG: u64 = 1 << 63;

/// 值引用 (含前驱指针用于历史遍历)
/// Value reference with prev pointer for history traversal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValRef {
  pub file_id: u64,
  pub offset: u64,       // 最高位=1 表示 tombstone
  pub prev_file_id: u64, // 0 = 无前驱 / 0 = no prev
  pub prev_offset: u64,
}

impl ValRef {
  /// 是否为 tombstone / Check if tombstone
  #[inline]
  pub fn is_tombstone(&self) -> bool {
    self.offset & TOMBSTONE_FLAG != 0
  }

  /// 获取实际 offset / Get real offset
  #[inline]
  pub fn real_offset(&self) -> u64 {
    self.offset & !TOMBSTONE_FLAG
  }

  /// 是否有前驱 / Check if has prev
  #[inline]
  pub fn has_prev(&self) -> bool {
    self.prev_file_id != 0
  }
}

pub type Prev = ValRef;

pub enum Rt {
  Get(Option<Bytes>),
  Put(Option<Prev>),
  Rm(bool),
}

pub trait Pipeline: Sized {
  type Result<T>;

  fn rm<K: Into<Bytes>>(&self, key: K);

  fn put<K: Into<Bytes>, V: Into<Bytes>>(&self, key: K, val: V);

  fn get<K: Into<Bytes>>(&self, key: K);

  fn exec(&self) -> impl Future<Output = impl Stream<Item = Self::Result<Rt>>>;
}

/// Table trait (sub-database) / 表 trait（子数据库）
pub trait Table: Sized {
  type Result<T>;
  type Conf;
  type Pipeline;

  fn pipeline(&self) -> Self::Pipeline;

  fn conf(conf: &[Self::Conf]) -> impl Future<Output = Self::Result<()>>;

  fn fork(&self, rev: Rev, order: Order) -> impl Future<Output = Self::Result<Option<Self>>>;

  fn last_rev(&self) -> Rev;

  fn put(
    &self,
    key: impl Into<Bytes>,
    val: impl Into<Bytes>,
  ) -> impl Future<Output = Self::Result<Option<Prev>>>;

  fn val(&self, val: ValRef) -> impl Future<Output = Self::Result<Option<Bytes>>>;

  fn get(&self, key: impl Into<Bytes>) -> impl Future<Output = Self::Result<Option<Bytes>>>;

  fn rm(&self, key: impl Into<Bytes>) -> impl Future<Output = Self::Result<()>>;

  fn history(
    &self,
    key: impl Into<Bytes>,
  ) -> impl Future<Output = Self::Result<impl Stream<Item = ValRef>>>;

  fn scan(
    &self,
    key: impl Into<Bytes>,
    order: Order,
  ) -> impl Future<Output = Self::Result<impl Stream<Item = Kv>>> {
    self._scan(key, order, self.last_rev())
  }

  fn _scan(
    &self,
    key: impl Into<Bytes>,
    order: Order,
    recv: Rev,
  ) -> impl Future<Output = Self::Result<impl Stream<Item = Kv>>>;
}

/// Jdb trait (database with multiple tables) / Jdb trait（含多表的数据库）
pub trait Jdb: Sized {
  type Result<T>;
  type Table: Table;
  type OpenConf;

  fn open(conf: &[Self::OpenConf]) -> impl Future<Output = Self::Result<Self>>;

  fn table(
    &self,
    id: TableId,
    conf: &[<Self::Table as Table>::Conf],
  ) -> impl Future<Output = Self::Result<Self::Table>>;

  fn fork(&self, id: TableId) -> impl Future<Output = Self::Result<Option<Self::Table>>>;

  fn scan(
    &self,
    start: TableId,
    order: Order,
  ) -> impl Future<Output = impl Stream<Item = Self::Result<Self::Table>>>;
}
