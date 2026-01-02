//! Compaction - LSM-Tree compaction implementation
//! 压缩 - LSM-Tree 压缩实现
//!
//! Merges SSTables to reduce read amplification and reclaim space.
//! 合并 SSTable 以减少读放大并回收空间。

use std::path::{Path, PathBuf};

use jdb_fs::{FileLru, fs_id::id_path};

use super::{Entry, Level};
use crate::{Result, SSTableWriter};

/// Compaction result
/// 压缩结果
pub struct CompactResult {
  /// New SSTable IDs created
  /// 创建的新 SSTable ID
  pub new_tables: Vec<u64>,
  /// Old SSTable IDs to remove
  /// 要移除的旧 SSTable ID
  pub old_tables: Vec<u64>,
  /// Destination level
  /// 目标层级
  pub dest_level: usize,
}

/// Check if L0 compaction is needed
/// 检查是否需要 L0 压缩
#[inline]
pub fn needs_l0_compaction(l0_count: usize, threshold: usize) -> bool {
  l0_count >= threshold
}

/// Check if level compaction is needed
/// 检查是否需要层级压缩
pub fn needs_level_compaction(
  level_size: u64,
  level_idx: usize,
  base_size: u64,
  ratio: usize,
) -> bool {
  if level_idx == 0 {
    return false;
  }
  let target = level_target_size(level_idx, base_size, ratio);
  level_size > target
}

/// Calculate target size for a level
/// 计算层级的目标大小
pub fn level_target_size(level_idx: usize, base_size: u64, ratio: usize) -> u64 {
  if level_idx <= 1 {
    base_size
  } else {
    let mut size = base_size;
    for _ in 1..level_idx {
      size = size.saturating_mul(ratio as u64);
    }
    size
  }
}

/// Find overlapping tables in a level for a given key range
/// 在层级中查找与给定键范围重叠的表
pub fn find_overlapping_tables(level: &Level, min_key: &[u8], max_key: &[u8]) -> Vec<usize> {
  let mut indices = Vec::new();
  for (idx, table) in level.tables.iter().enumerate() {
    let meta = table.meta();
    if meta.max_key.as_ref() >= min_key && meta.min_key.as_ref() <= max_key {
      indices.push(idx);
    }
  }
  indices
}

/// Merge entry from multiple sources, keeping newest version
/// 从多个源合并条目，保留最新版本
pub struct CompactMerger {
  entries: Vec<(Box<[u8]>, Entry)>,
}

impl CompactMerger {
  /// Create merger from multiple SSTable iterators
  /// 从多个 SSTable 迭代器创建合并器
  pub fn new<I>(sources: Vec<I>) -> Self
  where
    I: IntoIterator<Item = (Box<[u8]>, Entry)>,
  {
    let mut all: Vec<(Box<[u8]>, Entry, usize)> = Vec::new();

    for (priority, source) in sources.into_iter().enumerate() {
      for (key, entry) in source {
        all.push((key, entry, priority));
      }
    }

    all.sort_by(|a, b| match a.0.cmp(&b.0) {
      std::cmp::Ordering::Equal => a.2.cmp(&b.2),
      other => other,
    });

    let mut entries: Vec<(Box<[u8]>, Entry)> = Vec::new();
    let mut last_key: Option<Box<[u8]>> = None;

    for (key, entry, _) in all {
      if last_key
        .as_ref()
        .is_some_and(|k| k.as_ref() == key.as_ref())
      {
        continue;
      }
      last_key = Some(key.clone());
      entries.push((key, entry));
    }

    Self { entries }
  }

  /// Get merged entries iterator
  /// 获取合并条目迭代器
  pub fn iter(self, skip_tombstones: bool) -> impl Iterator<Item = (Box<[u8]>, Entry)> {
    self.entries.into_iter().filter(move |(_, entry)| {
      if skip_tombstones {
        !entry.is_tombstone()
      } else {
        true
      }
    })
  }
}

/// Compact L0 tables into L1
/// 将 L0 表压缩到 L1
pub async fn compact_l0_to_l1(
  sst_dir: &Path,
  l0: &Level,
  l1: &Level,
  next_table_id: &mut u64,
  files: &mut FileLru,
) -> Result<CompactResult> {
  if l0.is_empty() {
    return Ok(CompactResult {
      new_tables: Vec::new(),
      old_tables: Vec::new(),
      dest_level: 1,
    });
  }

  let l0_ids: Vec<u64> = l0.tables.iter().map(|t| t.meta().id).collect();

  // Find key range of all L0 tables
  // 找到所有 L0 表的键范围
  let mut min_key: Option<Box<[u8]>> = None;
  let mut max_key: Option<Box<[u8]>> = None;

  for table in &l0.tables {
    let meta = table.meta();
    if min_key.is_none()
      || meta.min_key.as_ref() < min_key.as_ref().map(|k| k.as_ref()).unwrap_or(&[])
    {
      min_key = Some(meta.min_key.clone());
    }
    if max_key.is_none()
      || meta.max_key.as_ref() > max_key.as_ref().map(|k| k.as_ref()).unwrap_or(&[])
    {
      max_key = Some(meta.max_key.clone());
    }
  }

  let min_key = min_key.unwrap_or_default();
  let max_key = max_key.unwrap_or_default();

  let l1_indices = find_overlapping_tables(l1, &min_key, &max_key);
  let l1_ids: Vec<u64> = l1_indices.iter().map(|&i| l1.tables[i].meta().id).collect();

  let mut sources: Vec<Vec<(Box<[u8]>, Entry)>> = Vec::new();

  // L0 tables in reverse order (newest first)
  // L0 表按逆序（最新的优先）
  for table in l0.tables.iter().rev() {
    let iter = table.iter_with_tombstones(files).await?;
    sources.push(iter.collect());
  }

  for &idx in &l1_indices {
    let iter = l1.tables[idx].iter_with_tombstones(files).await?;
    sources.push(iter.collect());
  }

  let merger = CompactMerger::new(sources);
  let entries: Vec<_> = merger.iter(false).collect();

  if entries.is_empty() {
    return Ok(CompactResult {
      new_tables: Vec::new(),
      old_tables: l0_ids.into_iter().chain(l1_ids).collect(),
      dest_level: 1,
    });
  }

  let table_id = *next_table_id;
  *next_table_id += 1;

  let path = sstable_path(sst_dir, table_id);
  let mut writer = SSTableWriter::new(path, table_id, entries.len()).await?;

  for (key, entry) in &entries {
    writer.add(key, entry).await?;
  }

  writer.finish().await?;

  Ok(CompactResult {
    new_tables: vec![table_id],
    old_tables: l0_ids.into_iter().chain(l1_ids).collect(),
    dest_level: 1,
  })
}

/// Compact tables from one level to the next
/// 将表从一个层级压缩到下一个层级
pub async fn compact_level(
  sst_dir: &Path,
  src_level: &Level,
  dst_level: &Level,
  next_table_id: &mut u64,
  skip_tombstones: bool,
  files: &mut FileLru,
) -> Result<CompactResult> {
  if src_level.is_empty() {
    return Ok(CompactResult {
      new_tables: Vec::new(),
      old_tables: Vec::new(),
      dest_level: dst_level.level,
    });
  }

  let src_table = &src_level.tables[0];
  let src_meta = src_table.meta();
  let src_id = src_meta.id;

  let dst_indices = find_overlapping_tables(dst_level, &src_meta.min_key, &src_meta.max_key);
  let dst_ids: Vec<u64> = dst_indices
    .iter()
    .map(|&i| dst_level.tables[i].meta().id)
    .collect();

  let mut sources: Vec<Vec<(Box<[u8]>, Entry)>> = Vec::new();

  let iter = src_table.iter_with_tombstones(files).await?;
  sources.push(iter.collect());

  for &idx in &dst_indices {
    let iter = dst_level.tables[idx].iter_with_tombstones(files).await?;
    sources.push(iter.collect());
  }

  let merger = CompactMerger::new(sources);
  let entries: Vec<_> = merger.iter(skip_tombstones).collect();

  if entries.is_empty() {
    return Ok(CompactResult {
      new_tables: Vec::new(),
      old_tables: vec![src_id].into_iter().chain(dst_ids).collect(),
      dest_level: dst_level.level,
    });
  }

  let table_id = *next_table_id;
  *next_table_id += 1;

  let path = sstable_path(sst_dir, table_id);
  let mut writer = SSTableWriter::new(path, table_id, entries.len()).await?;

  for (key, entry) in &entries {
    writer.add(key, entry).await?;
  }

  writer.finish().await?;

  Ok(CompactResult {
    new_tables: vec![table_id],
    old_tables: vec![src_id].into_iter().chain(dst_ids).collect(),
    dest_level: dst_level.level,
  })
}

fn sstable_path(sst_dir: &Path, id: u64) -> PathBuf {
  id_path(sst_dir, id)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_level_target_size() {
    let base = 64 * 1024 * 1024;
    let ratio = 10;

    assert_eq!(level_target_size(0, base, ratio), base);
    assert_eq!(level_target_size(1, base, ratio), base);
    assert_eq!(level_target_size(2, base, ratio), base * 10);
    assert_eq!(level_target_size(3, base, ratio), base * 100);
    assert_eq!(level_target_size(4, base, ratio), base * 1000);
  }

  #[test]
  fn test_needs_l0_compaction() {
    assert!(!needs_l0_compaction(0, 4));
    assert!(!needs_l0_compaction(3, 4));
    assert!(needs_l0_compaction(4, 4));
    assert!(needs_l0_compaction(5, 4));
  }

  #[test]
  fn test_needs_level_compaction() {
    let base = 64 * 1024 * 1024;
    let ratio = 10;

    assert!(!needs_level_compaction(base * 100, 0, base, ratio));
    assert!(!needs_level_compaction(base, 1, base, ratio));
    assert!(needs_level_compaction(base + 1, 1, base, ratio));
    assert!(!needs_level_compaction(base * 10, 2, base, ratio));
    assert!(needs_level_compaction(base * 10 + 1, 2, base, ratio));
  }
}
