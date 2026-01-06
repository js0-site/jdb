//! Table range filter utilities
//! 表范围过滤工具

use std::ops::Bound;

use super::{Key, to_owned};
use crate::TableInfo;

/// Check if table range overlaps with query range
/// 检查表范围是否与查询范围重叠
#[inline]
fn overlaps(info: &TableInfo, start: &Bound<Key>, end: &Bound<Key>) -> bool {
  let meta = info.meta();
  // Check: NOT (start > max_key OR end < min_key)
  // 检查：NOT (start > max_key OR end < min_key)
  let start_past = match start {
    Bound::Unbounded => false,
    Bound::Included(k) => k.as_ref() > meta.max_key.as_ref(),
    Bound::Excluded(k) => k.as_ref() >= meta.max_key.as_ref(),
  };
  if start_past {
    return false;
  }
  let end_before = match end {
    Bound::Unbounded => false,
    Bound::Included(k) => k.as_ref() < meta.min_key.as_ref(),
    Bound::Excluded(k) => k.as_ref() <= meta.min_key.as_ref(),
  };
  !end_before
}

/// Filter tables that overlap with range, returns indices
/// 过滤与范围重叠的表，返回索引
pub fn filter_tables(tables: &[TableInfo], start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<usize> {
  let start = to_owned(start);
  let end = to_owned(end);
  tables
    .iter()
    .enumerate()
    .filter(|(_, t)| overlaps(t, &start, &end))
    .map(|(i, _)| i)
    .collect()
}
