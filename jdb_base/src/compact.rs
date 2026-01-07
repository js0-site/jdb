//! Compact trait for compaction operations
//! 压缩操作的 trait

use std::future::Future;

use crate::table::Meta;

/// Compact trait for merging tables
/// 合并表的压缩 trait
pub trait Compact<T: Meta> {
  /// Error type / 错误类型
  type Error;

  /// Merge L0 tables (multiple src files, may overlap)
  /// 合并 L0 表（多个源文件，可能重叠）
  ///
  /// - src_ids: L0 table ids to merge / 要合并的 L0 表 id
  /// - dst_ids: destination table ids to merge / 要合并的目标表 id
  /// - dst_level: target level / 目标层级
  ///
  /// Returns new table metas
  /// 返回新表元数据
  fn merge_l0(
    &mut self,
    src_ids: &[u64],
    dst_ids: &[u64],
    dst_level: u8,
  ) -> impl Future<Output = Result<Vec<T>, Self::Error>>;

  /// Merge single table from L1+ (one src file, no overlap in src level)
  /// 合并 L1+ 的单个表（一个源文件，源层无重叠）
  ///
  /// - src_level: source level / 源层级
  /// - src_id: source table id / 源表 id
  /// - dst_ids: destination table ids to merge / 要合并的目标表 id
  /// - dst_level: target level / 目标层级
  ///
  /// Returns new table metas
  /// 返回新表元数据
  fn merge(
    &mut self,
    src_level: u8,
    src_id: u64,
    dst_ids: &[u64],
    dst_level: u8,
  ) -> impl Future<Output = Result<Vec<T>, Self::Error>>;

  /// Remove physical file after compaction committed
  /// 压缩提交后删除物理文件
  fn rm(&mut self, id: u64) -> impl Future<Output = Result<(), Self::Error>>;
}
