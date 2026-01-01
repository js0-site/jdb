//! Level - SSTable level management
//! 层级 - SSTable 层级管理
//!
//! Manages SSTables at each level of the LSM-Tree.
//! 管理 LSM-Tree 每一层的 SSTable。

use crate::{SSTableReader, TableMeta};

/// Level in LSM-Tree
/// LSM-Tree 中的层级
pub struct Level {
  /// Level number (0 = L0)
  /// 层级编号（0 = L0）
  pub level: usize,
  /// SSTables in this level
  /// 此层级的 SSTable
  pub tables: Vec<SSTableReader>,
}

impl Level {
  /// Create new empty level
  /// 创建新的空层级
  #[inline]
  pub fn new(level: usize) -> Self {
    Self {
      level,
      tables: Vec::new(),
    }
  }

  /// Add SSTable to level
  /// 添加 SSTable 到层级
  #[inline]
  pub fn add(&mut self, table: SSTableReader) {
    self.tables.push(table);
  }

  /// Get table count
  /// 获取表数量
  #[inline]
  pub fn len(&self) -> usize {
    self.tables.len()
  }

  /// Check if empty
  /// 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.tables.is_empty()
  }

  /// Get total size in bytes
  /// 获取总大小（字节）
  pub fn size(&self) -> u64 {
    self.tables.iter().map(|t| t.meta().file_size).sum()
  }

  /// Get all table metadata
  /// 获取所有表元数据
  pub fn metas(&self) -> Vec<&TableMeta> {
    self.tables.iter().map(|t| t.meta()).collect()
  }
}
