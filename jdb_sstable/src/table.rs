//! AsyncTable trait implementation for SSTable
//! SSTable 的 AsyncTable trait 实现

use std::ops::Bound;

use jdb_base::{Pos, table::AsyncTable};
use jdb_fs::FileLru;

use crate::{
  TableInfo, TableMeta,
  stream::{AscStream, DescStream},
};

/// SSTable with async query interface (O(1) memory)
/// 带异步查询接口的 SSTable（O(1) 内存）
pub struct SSTable<'a> {
  info: TableInfo,
  file_lru: &'a mut FileLru,
}

impl<'a> SSTable<'a> {
  /// Create SSTable (no preloading, O(1) memory)
  /// 创建 SSTable（无预加载，O(1) 内存）
  #[inline]
  pub fn new(info: TableInfo, file_lru: &'a mut FileLru) -> Self {
    Self { info, file_lru }
  }

  /// Get metadata
  /// 获取元数据
  #[inline]
  pub fn meta(&self) -> &TableMeta {
    self.info.meta()
  }

  /// Get table info
  /// 获取表信息
  #[inline]
  pub fn info(&self) -> &TableInfo {
    &self.info
  }

  /// Check if key may exist (bloom filter)
  /// 检查键是否可能存在（布隆过滤器）
  #[inline]
  pub fn may_contain(&self, key: &[u8]) -> bool {
    self.info.may_contain(key)
  }

  /// Check if key is in range
  /// 检查键是否在范围内
  #[inline]
  pub fn is_key_in_range(&self, key: &[u8]) -> bool {
    self.info.is_key_in_range(key)
  }

  /// Get block count
  /// 获取块数量
  #[inline]
  pub fn block_count(&self) -> usize {
    self.info.block_count()
  }

  /// Check if empty
  /// 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.info.block_count() == 0
  }

  /// Calculate block index from bound
  /// 从边界计算块索引
  #[inline]
  fn bound_to_idx(&self, bound: Bound<&[u8]>, default: usize) -> usize {
    match bound {
      Bound::Included(k) | Bound::Excluded(k) => self.info.find_block(k),
      Bound::Unbounded => default,
    }
  }
}

impl AsyncTable for SSTable<'_> {
  type RangeStream<'b>
    = AscStream<'b>
  where
    Self: 'b;
  type RevStream<'b>
    = DescStream<'b>
  where
    Self: 'b;

  async fn get(&mut self, key: &[u8]) -> Option<Pos> {
    self.info.get_pos(key, self.file_lru).await.ok().flatten()
  }

  fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RangeStream<'_> {
    let last = self.info.block_count().saturating_sub(1);
    let start_idx = self.bound_to_idx(start, 0);
    let end_idx = self.bound_to_idx(end, last);
    AscStream::new(&self.info, self.file_lru, start_idx, end_idx, start, end)
  }

  fn rev_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RevStream<'_> {
    let last = self.info.block_count().saturating_sub(1);
    let start_idx = self.bound_to_idx(start, 0);
    let end_idx = self.bound_to_idx(end, last);
    DescStream::new(&self.info, self.file_lru, start_idx, end_idx, start, end)
  }
}
