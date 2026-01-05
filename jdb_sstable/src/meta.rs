//! SSTable metadata
//! SSTable 元数据

/// Table metadata
/// 表元数据
#[derive(Debug, Clone, Default)]
pub struct TableMeta {
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

impl TableMeta {
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

  /// Check if key is in range [min_key, max_key]
  /// 检查键是否在范围内
  #[inline]
  pub fn contains_key(&self, key: &[u8]) -> bool {
    !self.min_key.is_empty() && key >= &*self.min_key && key <= &*self.max_key
  }
}
