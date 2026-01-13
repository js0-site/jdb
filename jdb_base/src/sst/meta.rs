//! SSTable metadata
//! SSTable 元数据

use std::{cmp::Ordering, ops::Bound};

use bitcode::{Decode, Encode};

/// Table metadata
/// 表元数据
#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct Meta {
  /// Table ID / 表 ID
  pub id: u64,
  /// Level number (0 = L0, 1 = L1, ...)
  /// 层级编号
  pub level: u8,
  /// 内部Pos的最大版本号
  pub max_ver: u64,
  /// Minimum key / 最小键
  pub min: Box<[u8]>,
  /// Maximum key / 最大键
  pub max: Box<[u8]>,
  /// Item count / 条目数量
  pub count: u64,
  /// Tombstone size (key_len + val_len + overhead)
  /// 墓碑大小（key_len + val_len + 固定开销）
  pub rmed_size: u64,
  /// File size / 文件大小
  pub file_size: u64,
}

impl PartialEq for Meta {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for Meta {}

impl PartialOrd for Meta {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Meta {
  // Sort by level asc, then max_ver desc, then id desc
  // 按 level 升序，max_ver 降序，id 降序排序
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .level
      .cmp(&other.level)
      .then_with(|| other.max_ver.cmp(&self.max_ver))
      .then_with(|| other.id.cmp(&self.id))
  }
}

impl Meta {
  #[inline]
  pub fn new(id: u64) -> Self {
    Self {
      id,
      level: 0,
      max_ver: 0,
      min: Box::default(),
      max: Box::default(),
      count: 0,
      rmed_size: 0,
      file_size: 0,
    }
  }

  /// Check if this table contains the key
  /// 检查表是否包含键
  #[inline]
  pub fn contains(&self, key: &[u8]) -> bool {
    key >= self.min.as_ref() && key <= self.max.as_ref()
  }

  /// Check if this table overlaps with the given range
  /// 检查表是否与给定范围重叠
  pub fn overlaps(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let (start_cmp, end_cmp) = match (start, end) {
      (Bound::Included(s), Bound::Included(e)) => (self.min.as_ref() <= e, self.max.as_ref() >= s),
      (Bound::Included(s), Bound::Excluded(e)) => (self.min.as_ref() < e, self.max.as_ref() >= s),
      (Bound::Excluded(s), Bound::Included(e)) => (self.min.as_ref() <= e, self.max.as_ref() > s),
      (Bound::Excluded(s), Bound::Excluded(e)) => (self.min.as_ref() < e, self.max.as_ref() > s),
      (Bound::Unbounded, Bound::Included(e)) => (true, self.max.as_ref() >= e),
      (Bound::Unbounded, Bound::Excluded(e)) => (true, self.max.as_ref() > e),
      (Bound::Included(s), Bound::Unbounded) => (self.min.as_ref() <= s, true),
      (Bound::Excluded(s), Bound::Unbounded) => (self.min.as_ref() < s, true),
      (Bound::Unbounded, Bound::Unbounded) => (true, true),
    };
    start_cmp && end_cmp
  }

  /// Calculate compensated size (file_size - rmed_size)
  /// 计算补偿大小（file_size - rmed_size）
  #[inline]
  pub fn size_without_rmed(&self) -> u64 {
    self.file_size.saturating_sub(self.rmed_size)
  }
}
