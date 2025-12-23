use std::time::Duration;

use hipstr::HipByt;

use crate::{Col, ColIdx, Val};

// ========================================================================
// Schema 定义
// ========================================================================

#[derive(Clone, Debug)]
pub struct Field {
  pub name: Col,
  pub default: Val,
}

#[derive(Clone, Debug)]
pub struct Index {
  pub cols: Vec<ColIdx>,
  pub unique: bool,
}

/// Schema 管理列名到偏移量的映射
/// Schema manages column name to offset mapping
#[derive(Clone, Debug)]
pub struct Schema {
  pub name: HipByt<'static>,
  pub col_li: Vec<Field>,
  pub sub_table_key_li: Vec<Field>,
  pub index_li: Vec<Index>,
  pub max_depth: Option<usize>,
  pub ttl: Option<Duration>,
}

impl Schema {
  /// 获取数据列偏移量 Get data column offset
  #[inline]
  pub fn col_idx(&self, name: &[u8]) -> Option<ColIdx> {
    self
      .col_li
      .iter()
      .enumerate() // 携带原始索引
      .find(|(_, f)| f.name.as_slice() == name) // 按名字找
      .and_then(|(i, _)| i.try_into().ok()) // 安全转换，防止截断
  }

  /// 获取子表键列偏移量 Get sub-table key column offset
  #[inline]
  pub fn sub_idx(&self, name: &[u8]) -> Option<ColIdx> {
    self
      .sub_table_key_li
      .iter()
      .enumerate() // 携带原始索引
      .find(|(_, f)| f.name.as_slice() == name) // 按名字找
      .and_then(|(i, _)| i.try_into().ok()) // 安全转换，防止截断
  }
}
