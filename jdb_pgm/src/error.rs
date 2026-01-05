//! Error definitions for PGM-Index
//! PGM 索引错误定义

use std::fmt;

pub type Result<T> = std::result::Result<T, PGMError>;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PGMError {
  EmptyData,
  NotSorted,
  InvalidEpsilon { provided: usize, min: usize },
}

impl fmt::Display for PGMError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::EmptyData => write!(f, "Data cannot be empty / 数据不能为空"),
      Self::NotSorted => write!(f, "Data must be sorted / 数据必须已排序"),
      Self::InvalidEpsilon { provided, min } => write!(
        f,
        "Epsilon must be >= {min} (provided: {provided}) / Epsilon 必须 >= {min} (提供的值: {provided})"
      ),
    }
  }
}

impl std::error::Error for PGMError {}
