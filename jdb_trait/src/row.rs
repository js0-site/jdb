use std::{fmt::Debug, future::Future};

use crate::Val;

/// 同步行数据 Synchronous row data
pub type Row = Vec<Val>;

/// 异步行数据 trait，用于键值分离场景
/// Async row data trait for key-value separation scenarios
pub trait AsyncRow: Send + Sync + Debug {
  type Error: Debug + Send + Sync;

  /// 异步获取完整行数据 Async get full row data
  fn row(&self) -> impl Future<Output = Result<Row, Self::Error>> + Send;
}
