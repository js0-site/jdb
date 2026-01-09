//! SSTable metadata
//! SSTable 元数据

use std::ops::Bound;

/// Table metadata
/// 表元数据
#[derive(Debug, Clone, Default)]
pub struct Meta {
  /// Table ID / 表 ID
  pub id: u64,
  /// Minimum key / 最小键
  pub min_key: Box<[u8]>,
  /// Maximum key / 最大键
  pub max_key: Box<[u8]>,
  /// Item count / 条目数量
  pub item_count: u64,
  /// Tombstone size (key_len + val_len + overhead)
  /// 墓碑大小（key_len + val_len + 固定开销）
  pub rmed_size: u64,
  /// File size / 文件大小
  pub file_size: u64,
}

impl Meta {
  #[inline]
  pub fn new(id: u64) -> Self {
    Self {
      id,
      min_key: Box::default(),
      max_key: Box::default(),
      item_count: 0,
      rmed_size: 0,
      file_size: 0,
    }
  }

  /// Check if this table contains the key
  /// 检查表是否包含键
  #[inline]
  pub fn contains(&self, key: &[u8]) -> bool {
    key >= self.min_key.as_ref() && key <= self.max_key.as_ref()
  }

  /// Check if this table overlaps with the given range
  /// 检查表是否与给定范围重叠
  pub fn overlaps(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let (start_cmp, end_cmp) = match (start, end) {
      (Bound::Included(s), Bound::Included(e)) => {
        (self.min_key.as_ref() <= e, self.max_key.as_ref() >= s)
      }
      (Bound::Included(s), Bound::Excluded(e)) => {
        (self.min_key.as_ref() < e, self.max_key.as_ref() >= s)
      }
      (Bound::Excluded(s), Bound::Included(e)) => {
        (self.min_key.as_ref() <= e, self.max_key.as_ref() > s)
      }
      (Bound::Excluded(s), Bound::Excluded(e)) => {
        (self.min_key.as_ref() < e, self.max_key.as_ref() > s)
      }
      (Bound::Unbounded, Bound::Included(e)) => (true, self.max_key.as_ref() >= e),
      (Bound::Unbounded, Bound::Excluded(e)) => (true, self.max_key.as_ref() > e),
      (Bound::Included(s), Bound::Unbounded) => (self.min_key.as_ref() <= s, true),
      (Bound::Excluded(s), Bound::Unbounded) => (self.min_key.as_ref() < s, true),
      (Bound::Unbounded, Bound::Unbounded) => (true, true),
    };
    start_cmp && end_cmp
  }

  /// Calculate compensated size (file_size - rmed_size)
  /// 计算补偿大小（file_size - rmed_size）
  #[inline]
  pub fn compensated_size(&self) -> u64 {
    self.file_size.saturating_sub(self.rmed_size)
  }
}

impl AsRef<Meta> for Meta {
  #[inline]
  fn as_ref(&self) -> &Meta {
    self
  }
}
