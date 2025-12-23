#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod expr;
mod query;
mod schema;
mod sub_table;
mod table;
mod val;

use std::{fmt::Debug, future::Future};

pub use expr::{Expr, Op, Order};
use hipstr::HipByt;
pub use query::Query;
pub use schema::{Field, Index, Schema, SchemaVer};
pub use sub_table::SubTable;
pub use table::Table;
pub use val::Val;

/// 全局 ID 类型
pub type Id = u64;

/// 列名 Column name
pub type Col = HipByt<'static>;

/// 列偏移量 Column offset in Row
pub type ColIdx = u16;

/// 单行数据 (紧凑存储，列映射由 Schema 管理)
/// Row data (compact storage, column mapping managed by Schema)
pub type Row = Box<[Val]>;

/// 批次数据 Batch data
pub type Batch = Vec<Row>;

/// 子表键 (用于路由定位子表)
/// SubTable key for routing to sub-table partition
pub type SubTableKey = Row;

/// 记录 Record
#[derive(Debug, Clone)]
pub struct Record {
  pub sub_table: SubTableKey,
  pub id: Id,
  pub row: Row,
}

pub trait IdGen: Send + Sync {
  type Error: Debug + Send + Sync;
  fn get(&self) -> impl Future<Output = Result<Id, Self::Error>> + Send;
}

pub trait Engine: Sized + Send + Sync {
  type Error: Debug + Send + Sync;
  type Gen: IdGen;
  type Table: Table;

  fn id_gen(&self) -> &Self::Gen;

  /// 打开或创建表
  ///
  /// * `create`: 仅当表不存在时调用的异步回调 (Get or Create pattern)
  fn open<F, Fut>(
    &self,
    name: &[u8],
    create: F, // Renamed from on_create
  ) -> impl Future<Output = Result<Self::Table, Self::Error>> + Send
  where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Schema> + Send;
}
