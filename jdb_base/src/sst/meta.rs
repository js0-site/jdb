//! SSTable metadata
//! SSTable 元数据

use bitcode::{Decode, Encode};

/// Table metadata
/// 表元数据
#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct Meta {
  /// Table ID / 表 ID
  pub id: u64,
  /// Minimum key / 最小键
  pub min: Box<[u8]>,
  /// Maximum key / 最大键
  pub max: Box<[u8]>,
  /// Tombstone size (key_len + val_len + overhead)
  /// 墓碑大小（key_len + val_len + 固定开销）
  pub rmed_size: u64,
  /// File size / 文件大小
  pub file_size: u64,
}

impl PartialEq for Meta {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.cmp(other).is_eq()
  }
}

impl Eq for Meta {}

impl PartialOrd for Meta {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Meta {
  #[inline]
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.min.cmp(&other.min)
  }
}

impl Meta {
  #[inline]
  pub fn new(id: u64) -> Self {
    Self {
      id,
      min: Box::default(),
      max: Box::default(),
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

  /// Calculate compensated size (file_size - rmed_size)
  /// 计算补偿大小（file_size - rmed_size）
  #[inline]
  pub fn size_without_rmed(&self) -> u64 {
    self.file_size.saturating_sub(self.rmed_size)
  }
}

impl std::ops::RangeBounds<[u8]> for Meta {
  #[inline]
  fn start_bound(&self) -> std::ops::Bound<&[u8]> {
    std::ops::Bound::Included(&self.min)
  }

  #[inline]
  fn end_bound(&self) -> std::ops::Bound<&[u8]> {
    std::ops::Bound::Included(&self.max)
  }
}
