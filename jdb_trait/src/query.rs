use crate::expr::{Expr, Order};

#[derive(Clone, Default)]
pub struct Query {
  /// 子表过滤条件 SubTable filter for routing
  pub sub_table_filter: Option<Expr>,
  /// 索引/数据过滤条件 Index/data filter
  pub val_filter: Option<Expr>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
  pub order: Order,
}
