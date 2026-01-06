//! Async stream for SSTable range queries
//! SSTable 范围查询的异步流

mod asc;
mod desc;
mod merge;
mod multi;

use std::{cell::RefCell, ops::Bound, rc::Rc};

pub use asc::asc_stream;
pub use desc::desc_stream;
use jdb_fs::FileLru;
pub use merge::filter_tables;
pub use multi::{MultiAsc, MultiDesc};

use crate::TableInfo;

pub(crate) type Key = Box<[u8]>;
pub(crate) type Lru = Rc<RefCell<FileLru>>;

/// Convert bound reference to owned
/// 将边界引用转换为所有权
#[inline]
pub(crate) fn to_owned(bound: Bound<&[u8]>) -> Bound<Key> {
  match bound {
    Bound::Unbounded => Bound::Unbounded,
    Bound::Included(k) => Bound::Included(k.into()),
    Bound::Excluded(k) => Bound::Excluded(k.into()),
  }
}

/// Check if key exceeds end bound
/// 检查键是否超出结束边界
#[inline]
pub(crate) fn past_end(key: &[u8], end: &Bound<Key>) -> bool {
  match end {
    Bound::Unbounded => false,
    Bound::Included(k) => key > k.as_ref(),
    Bound::Excluded(k) => key >= k.as_ref(),
  }
}

/// Check if key is before start bound
/// 检查键是否在起始边界之前
#[inline]
pub(crate) fn before_start(key: &[u8], start: &Bound<Key>) -> bool {
  match start {
    Bound::Unbounded => false,
    Bound::Included(k) => key < k.as_ref(),
    Bound::Excluded(k) => key <= k.as_ref(),
  }
}

/// Compute block index from bound
/// 根据边界计算 block 索引
#[inline]
fn bound_to_idx(info: &TableInfo, bound: &Bound<Key>, default: usize) -> usize {
  match bound {
    Bound::Included(k) | Bound::Excluded(k) => info.find_block(k),
    Bound::Unbounded => default,
  }
}

/// Check if range has no overlap with table
/// 检查范围是否与表无交集
#[inline]
fn no_overlap(info: &TableInfo, start: &Bound<Key>, end: &Bound<Key>) -> bool {
  let meta = info.meta();
  // start > max_key => no overlap
  // start > max_key => 无交集
  let start_past = match start {
    Bound::Unbounded => false,
    Bound::Included(k) => k.as_ref() > meta.max_key.as_ref(),
    Bound::Excluded(k) => k.as_ref() >= meta.max_key.as_ref(),
  };
  if start_past {
    return true;
  }
  // end < min_key => no overlap
  // end < min_key => 无交集
  match end {
    Bound::Unbounded => false,
    Bound::Included(k) => k.as_ref() < meta.min_key.as_ref(),
    Bound::Excluded(k) => k.as_ref() <= meta.min_key.as_ref(),
  }
}

/// Common stream init params
/// 公共流初始化参数
pub(crate) struct StreamInit {
  pub start: Bound<Key>,
  pub end: Bound<Key>,
  pub start_idx: usize,
  pub end_idx: usize,
  pub empty: bool,
}

impl StreamInit {
  pub fn new(info: &TableInfo, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
    let start = to_owned(start);
    let end = to_owned(end);
    let empty = info.block_count() == 0 || no_overlap(info, &start, &end);
    let last = info.block_count().saturating_sub(1);
    let start_idx = if empty {
      0
    } else {
      bound_to_idx(info, &start, 0)
    };
    let end_idx = if empty {
      0
    } else {
      bound_to_idx(info, &end, last)
    };
    Self {
      start,
      end,
      start_idx,
      end_idx,
      empty,
    }
  }
}
