//! Level - SSTable level management
//! 层级 - SSTable 层级管理
//!
//! Manages SSTables at each level of the LSM-Tree.
//! 管理 LSM-Tree 每一层的 SSTable。

use crate::{TableInfo, TableMeta};

/// Level in LSM-Tree
/// LSM-Tree 中的层级
pub struct Level {
  /// Level number (0 = L0)
  /// 层级编号（0 = L0）
  pub level: usize,
  /// SSTables in this level
  /// 此层级的 SSTable
  pub tables: Vec<TableInfo>,
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
  ///
  /// For L0: append to end (newest last, searched in reverse)
  /// For L1+: insert in sorted order by min_key for binary search
  /// L0：追加到末尾（最新的在最后，反向搜索）
  /// L1+：按 min_key 排序插入以支持二分查找
  #[inline]
  pub fn add(&mut self, table: TableInfo) {
    if self.level == 0 {
      // L0: just append, tables may overlap
      // L0：直接追加，表可能重叠
      self.tables.push(table);
    } else {
      // L1+: insert in sorted order by min_key
      // L1+：按 min_key 排序插入
      let min_key = table.meta().min_key.clone();
      let pos = self
        .tables
        .partition_point(|t| t.meta().min_key.as_ref() < min_key.as_ref());
      self.tables.insert(pos, table);
    }
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
