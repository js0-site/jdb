use gxhash::HashSet;
use crate::{ColIdx, Val};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Order {
  #[default]
  Asc,
  Desc,
}

#[derive(Clone, Debug)]
pub enum Op {
  Eq(Val),
  // 使用 HashSet<Val> 替代 Box<[Val]> 以提供 O(1) 查找性能
  In(HashSet<Val>),
  /// [start, end) 左闭右开
  /// 注意：当 start > end 时应返回空集，而不是 panic
  Range(Val, Val),
  /// [start, end] 闭区间
  RangeInclusive(Val, Val),
  /// [start, +∞) 大于等于
  RangeFrom(Val),
  /// (-∞, end) 小于
  RangeTo(Val),
  /// (-∞, end] 小于等于
  RangeToInclusive(Val),
}

#[derive(Clone, Debug)]
pub enum Expr {
  // 引用 SubTableKey 的列
  KeyCol(ColIdx, Op),
  // 引用 Data Row 的列
  ValCol(ColIdx, Op),

  And(Box<Expr>, Box<Expr>),
  Or(Box<Expr>, Box<Expr>),
  Not(Box<Expr>), // 逻辑 NOT 运算
}

impl Expr {
  /// 创建 SubTableKey 列条件表达式
  pub fn key_col(col_idx: ColIdx, op: Op) -> Self {
    Expr::KeyCol(col_idx, op)
  }

  /// 创建 Data Row 列条件表达式
  pub fn val_col(col_idx: ColIdx, op: Op) -> Self {
    Expr::ValCol(col_idx, op)
  }

  /// AND 链式操作
  pub fn and(self, other: Expr) -> Self {
    Expr::And(Box::new(self), Box::new(other))
  }

  /// OR 链式操作
  pub fn or(self, other: Expr) -> Self {
    Expr::Or(Box::new(self), Box::new(other))
  }

  /// NOT 操作
  pub fn not(self) -> Self {
    Expr::Not(Box::new(self))
  }

  // SubTableKey 列的便捷方法
  /// 便捷方法：Key 列等于
  pub fn key_eq(col_idx: ColIdx, val: Val) -> Self {
    Self::key_col(col_idx, Op::Eq(val))
  }

  /// 便捷方法：Key 列在范围内
  pub fn key_in(col_idx: ColIdx, val_li: impl Into<HashSet<Val>>) -> Self {
    Self::key_col(col_idx, Op::In(val_li.into()))
  }

  /// 便捷方法：Key 列范围 [start, end)
  pub fn key_range(col_idx: ColIdx, start: Val, end: Val) -> Self {
    Self::key_col(col_idx, Op::Range(start, end))
  }

  /// 便捷方法：Key 列闭区间 [start, end]
  pub fn key_range_inclusive(col_idx: ColIdx, start: Val, end: Val) -> Self {
    Self::key_col(col_idx, Op::RangeInclusive(start, end))
  }

  /// 便捷方法：Key 列大于等于 [start, +∞)
  pub fn key_range_from(col_idx: ColIdx, start: Val) -> Self {
    Self::key_col(col_idx, Op::RangeFrom(start))
  }

  /// 便捷方法：Key 列小于 (-∞, end)
  pub fn key_range_to(col_idx: ColIdx, end: Val) -> Self {
    Self::key_col(col_idx, Op::RangeTo(end))
  }

  /// 便捷方法：Key 列小于等于 (-∞, end]
  pub fn key_range_to_inclusive(col_idx: ColIdx, end: Val) -> Self {
    Self::key_col(col_idx, Op::RangeToInclusive(end))
  }

  // Data Row 列的便捷方法
  /// 便捷方法：Value 列等于
  pub fn val_eq(col_idx: ColIdx, val: Val) -> Self {
    Self::val_col(col_idx, Op::Eq(val))
  }

  /// 便捷方法：Value 列在范围内
  pub fn val_in_range(col_idx: ColIdx, val_li: impl Into<HashSet<Val>>) -> Self {
    Self::val_col(col_idx, Op::In(val_li.into()))
  }

  /// 便捷方法：Value 列范围 [start, end)
  pub fn val_range(col_idx: ColIdx, start: Val, end: Val) -> Self {
    Self::val_col(col_idx, Op::Range(start, end))
  }

  /// 便捷方法：Value 列闭区间 [start, end]
  pub fn val_range_inclusive(col_idx: ColIdx, start: Val, end: Val) -> Self {
    Self::val_col(col_idx, Op::RangeInclusive(start, end))
  }

  /// 便捷方法：Value 列大于等于 [start, +∞)
  pub fn val_range_from(col_idx: ColIdx, start: Val) -> Self {
    Self::val_col(col_idx, Op::RangeFrom(start))
  }

  /// 便捷方法：Value 列小于 (-∞, end)
  pub fn val_range_to(col_idx: ColIdx, end: Val) -> Self {
    Self::val_col(col_idx, Op::RangeTo(end))
  }

  /// 便捷方法：Value 列小于等于 (-∞, end]
  pub fn val_range_to_inclusive(col_idx: ColIdx, end: Val) -> Self {
    Self::val_col(col_idx, Op::RangeToInclusive(end))
  }
}
