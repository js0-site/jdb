//! SSTable metadata
//! SSTable 元数据

use jdb_base::table;

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
      file_size: 0,
    }
  }
}

impl table::Meta for Meta {
  #[inline]
  fn id(&self) -> u64 {
    self.id
  }

  #[inline]
  fn min_key(&self) -> &[u8] {
    &self.min_key
  }

  #[inline]
  fn max_key(&self) -> &[u8] {
    &self.max_key
  }

  #[inline]
  fn size(&self) -> u64 {
    self.file_size
  }

  #[inline]
  fn count(&self) -> u64 {
    self.item_count
  }
}
