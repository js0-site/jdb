//! Index - LSM-Tree index implementation
//! 索引 - LSM-Tree 索引实现
//!
//! Manages memtable, sealed memtables, and SSTable levels.
//! 管理内存表、密封内存表和 SSTable 层级。

use std::{ops::Bound, path::PathBuf};

use jdb_base::Pos;

use super::{
  Entry, Level, Memtable, MergeIter,
  compact::{compact_l0_to_l1, compact_level, needs_l0_compaction, needs_level_compaction},
};
use crate::{Conf, Result, SSTableReader, SSTableWriter};

/// LSM-Tree index
/// LSM-Tree 索引
pub struct Index {
  /// Directory for SSTable files
  /// SSTable 文件目录
  dir: PathBuf,
  /// Active memtable for writes
  /// 活跃内存表（用于写入）
  memtable: Memtable,
  /// Sealed memtables waiting for flush
  /// 等待刷新的密封内存表
  sealed: Vec<Memtable>,
  /// SSTable levels (L0, L1, ...)
  /// SSTable 层级
  levels: Vec<Level>,
  /// Next table ID
  /// 下一个表 ID
  next_table_id: u64,
  /// Next memtable ID
  /// 下一个内存表 ID
  next_memtable_id: u64,
  /// Configuration
  /// 配置
  conf: Conf,
}

impl Index {
  /// Create new index
  /// 创建新索引
  pub fn new(dir: PathBuf, conf: Conf) -> Self {
    Self {
      dir,
      memtable: Memtable::new(0),
      sealed: Vec::new(),
      levels: vec![Level::new(0)], // Start with L0
      next_table_id: 1,
      next_memtable_id: 1,
      conf,
    }
  }

  /// Get entry by key
  /// 按键获取条目
  ///
  /// Search order: memtable -> sealed memtables -> L0 -> L1 -> ...
  /// 搜索顺序：内存表 -> 密封内存表 -> L0 -> L1 -> ...
  pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
    // 1. Search active memtable
    // 1. 搜索活跃内存表
    if let Some(entry) = self.memtable.get(key) {
      return Ok(Some(*entry));
    }

    // 2. Search sealed memtables (newest first)
    // 2. 搜索密封内存表（最新的优先）
    for sealed in self.sealed.iter().rev() {
      if let Some(entry) = sealed.get(key) {
        return Ok(Some(*entry));
      }
    }

    // 3. Search SSTable levels
    // 3. 搜索 SSTable 层级
    for level in &self.levels {
      // L0: tables may overlap, search all (newest first)
      // L0：表可能重叠，搜索所有（最新的优先）
      if level.level == 0 {
        for table in level.tables.iter().rev() {
          // Quick filter check
          // 快速过滤器检查
          if !table.is_key_in_range(key) {
            continue;
          }
          if !table.may_contain(key) {
            continue;
          }
          if let Some(entry) = table.get(key).await? {
            return Ok(Some(entry));
          }
        }
      } else {
        // L1+: tables are sorted and non-overlapping
        // L1+：表是有序且不重叠的
        // Binary search for the right table
        // 二分查找正确的表
        let idx = level
          .tables
          .partition_point(|t| t.meta().max_key.as_ref() < key);
        if idx < level.tables.len() {
          let table = &level.tables[idx];
          if table.is_key_in_range(key)
            && table.may_contain(key)
            && let Some(entry) = table.get(key).await?
          {
            return Ok(Some(entry));
          }
        }
      }
    }

    Ok(None)
  }

  /// Put key-value pair
  /// 插入键值对
  #[inline]
  pub fn put(&mut self, key: Box<[u8]>, pos: Pos) {
    self.memtable.put(key, pos);
  }

  /// Delete key (insert tombstone)
  /// 删除键（插入删除标记）
  #[inline]
  pub fn del(&mut self, key: Box<[u8]>) {
    self.memtable.del(key);
  }

  /// Check if memtable should be flushed
  /// 检查内存表是否应该刷新
  #[inline]
  pub fn should_flush(&self) -> bool {
    self.memtable.size() >= self.conf.memtable_size
  }

  /// Get memtable size
  /// 获取内存表大小
  #[inline]
  pub fn memtable_size(&self) -> u64 {
    self.memtable.size()
  }

  /// Get total sealed memtable count
  /// 获取密封内存表总数
  #[inline]
  pub fn sealed_count(&self) -> usize {
    self.sealed.len()
  }

  /// Get L0 table count
  /// 获取 L0 表数量
  #[inline]
  pub fn l0_count(&self) -> usize {
    self.levels.first().map(|l| l.len()).unwrap_or(0)
  }

  /// Seal current memtable and create new one
  /// 密封当前内存表并创建新的
  pub fn seal_memtable(&mut self) {
    if self.memtable.is_empty() {
      return;
    }

    let old = std::mem::replace(&mut self.memtable, Memtable::new(self.next_memtable_id));
    self.next_memtable_id += 1;
    self.sealed.push(old);
  }

  /// Flush oldest sealed memtable to SSTable
  /// 将最旧的密封内存表刷新到 SSTable
  pub async fn flush_sealed(&mut self) -> Result<Option<u64>> {
    if self.sealed.is_empty() {
      return Ok(None);
    }

    // Take oldest sealed memtable
    // 取出最旧的密封内存表
    let memtable = self.sealed.remove(0);
    if memtable.is_empty() {
      return Ok(None);
    }

    // Create SSTable
    // 创建 SSTable
    let table_id = self.next_table_id;
    self.next_table_id += 1;

    let path = self.sstable_path(table_id);
    let mut writer = SSTableWriter::new(path.clone(), table_id, memtable.len()).await?;

    // Write all entries in sorted order
    // 按排序顺序写入所有条目
    for (key, entry) in memtable.iter() {
      writer.add(key, entry).await?;
    }

    let meta = writer.finish().await?;

    // Skip empty tables
    // 跳过空表
    if meta.item_count == 0 {
      return Ok(None);
    }

    // Load and add to L0
    // 加载并添加到 L0
    let reader = crate::SSTableReader::open(path, table_id).await?;
    if self.levels.is_empty() {
      self.levels.push(Level::new(0));
    }
    self.levels[0].add(reader);

    Ok(Some(table_id))
  }

  /// Generate SSTable file path
  /// 生成 SSTable 文件路径
  fn sstable_path(&self, id: u64) -> PathBuf {
    self.dir.join(format!("{id:08}.sst"))
  }

  /// Get directory
  /// 获取目录
  #[inline]
  pub fn dir(&self) -> &PathBuf {
    &self.dir
  }

  /// Get configuration
  /// 获取配置
  #[inline]
  pub fn conf(&self) -> &Conf {
    &self.conf
  }

  /// Get levels reference
  /// 获取层级引用
  #[inline]
  pub fn levels(&self) -> &[Level] {
    &self.levels
  }

  /// Get memtable reference
  /// 获取内存表引用
  #[inline]
  pub fn memtable(&self) -> &Memtable {
    &self.memtable
  }

  /// Get sealed memtables reference
  /// 获取密封内存表引用
  #[inline]
  pub fn sealed(&self) -> &[Memtable] {
    &self.sealed
  }

  /// Iterate all entries in range
  /// 迭代范围内的所有条目
  ///
  /// Merges results from memtable, sealed memtables, and SSTables.
  /// Skips tombstones by default.
  /// 合并来自内存表、密封内存表和 SSTable 的结果。
  /// 默认跳过删除标记。
  pub async fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<MergeIter> {
    let mut sources: Vec<Vec<(Box<[u8]>, Entry)>> = Vec::new();

    // 1. Active memtable (highest priority)
    // 1. 活跃内存表（最高优先级）
    let memtable_entries: Vec<_> = self
      .memtable
      .range(start, end)
      .map(|(k, e)| (k.into(), *e))
      .collect();
    sources.push(memtable_entries);

    // 2. Sealed memtables (newest first)
    // 2. 密封内存表（最新的优先）
    for sealed in self.sealed.iter().rev() {
      let entries: Vec<_> = sealed
        .range(start, end)
        .map(|(k, e)| (k.into(), *e))
        .collect();
      sources.push(entries);
    }

    // 3. SSTable levels
    // 3. SSTable 层级
    for level in &self.levels {
      for table in level.tables.iter().rev() {
        // Check if table overlaps with range
        // 检查表是否与范围重叠
        let meta = table.meta();
        let overlaps = match (start, end) {
          (Bound::Unbounded, Bound::Unbounded) => true,
          (Bound::Included(s), Bound::Unbounded) | (Bound::Excluded(s), Bound::Unbounded) => {
            meta.max_key.as_ref() >= s
          }
          (Bound::Unbounded, Bound::Included(e)) | (Bound::Unbounded, Bound::Excluded(e)) => {
            meta.min_key.as_ref() <= e
          }
          (Bound::Included(s), Bound::Included(e))
          | (Bound::Included(s), Bound::Excluded(e))
          | (Bound::Excluded(s), Bound::Included(e))
          | (Bound::Excluded(s), Bound::Excluded(e)) => {
            meta.max_key.as_ref() >= s && meta.min_key.as_ref() <= e
          }
        };

        if !overlaps {
          continue;
        }

        // Get range from SSTable
        // 从 SSTable 获取范围
        let (start_key, end_key) = bounds_to_keys(start, end);
        let iter = table.range(&start_key, &end_key).await?;
        let entries: Vec<_> = iter.collect();
        sources.push(entries);
      }
    }

    Ok(MergeIter::new(sources, true))
  }

  /// Iterate all entries with prefix
  /// 迭代所有带前缀的条目
  ///
  /// Returns all entries whose keys start with the given prefix.
  /// 返回所有键以给定前缀开头的条目。
  pub async fn prefix(&self, prefix: &[u8]) -> Result<MergeIter> {
    // Calculate prefix range: [prefix, prefix_end)
    // 计算前缀范围：[prefix, prefix_end)
    let end_bound = prefix_end_bound(prefix);

    let mut sources: Vec<Vec<(Box<[u8]>, Entry)>> = Vec::new();

    // 1. Active memtable (highest priority)
    // 1. 活跃内存表（最高优先级）
    let start = Bound::Included(prefix);
    let end = end_bound
      .as_ref()
      .map(|v| Bound::Excluded(v.as_slice()))
      .unwrap_or(Bound::Unbounded);

    let memtable_entries: Vec<_> = self
      .memtable
      .range(start, end)
      .map(|(k, e)| (k.into(), *e))
      .collect();
    sources.push(memtable_entries);

    // 2. Sealed memtables (newest first)
    // 2. 密封内存表（最新的优先）
    for sealed in self.sealed.iter().rev() {
      let entries: Vec<_> = sealed
        .range(start, end)
        .map(|(k, e)| (k.into(), *e))
        .collect();
      sources.push(entries);
    }

    // 3. SSTable levels
    // 3. SSTable 层级
    let start_key = prefix.to_vec();
    let end_key = end_bound.clone().unwrap_or_else(|| vec![0xff; 256]);

    for level in &self.levels {
      for table in level.tables.iter().rev() {
        // Check if table overlaps with prefix range
        // 检查表是否与前缀范围重叠
        let meta = table.meta();
        if meta.max_key.as_ref() < prefix {
          continue;
        }
        if let Some(ref end_vec) = end_bound
          && meta.min_key.as_ref() >= end_vec.as_slice()
        {
          continue;
        }

        let iter = table.range(&start_key, &end_key).await?;
        let entries: Vec<_> = iter.collect();
        sources.push(entries);
      }
    }

    Ok(MergeIter::new(sources, true))
  }

  /// Iterate all entries
  /// 迭代所有条目
  pub async fn iter(&self) -> Result<MergeIter> {
    self.range(Bound::Unbounded, Bound::Unbounded).await
  }

  /// Check if L0 compaction is needed
  /// 检查是否需要 L0 压缩
  #[inline]
  pub fn needs_l0_compaction(&self) -> bool {
    needs_l0_compaction(self.l0_count(), self.conf.l0_threshold)
  }

  /// Check if any level needs compaction
  /// 检查是否有任何层级需要压缩
  pub fn needs_compaction(&self) -> Option<usize> {
    // Check L0 first
    // 先检查 L0
    if self.needs_l0_compaction() {
      return Some(0);
    }

    // Check L1+ levels
    // 检查 L1+ 层级
    let base_size = self.conf.memtable_size * self.conf.l0_threshold as u64;
    for (idx, level) in self.levels.iter().enumerate().skip(1) {
      if needs_level_compaction(level.size(), idx, base_size, self.conf.level_ratio) {
        return Some(idx);
      }
    }

    None
  }

  /// Run L0 to L1 compaction
  /// 运行 L0 到 L1 压缩
  ///
  /// Merges all L0 tables with overlapping L1 tables.
  /// 将所有 L0 表与重叠的 L1 表合并。
  pub async fn compact_l0(&mut self) -> Result<bool> {
    if self.levels.is_empty() || self.levels[0].is_empty() {
      return Ok(false);
    }

    // Ensure L1 exists
    // 确保 L1 存在
    while self.levels.len() < 2 {
      self.levels.push(Level::new(self.levels.len()));
    }

    let result = compact_l0_to_l1(
      &self.dir,
      &self.levels[0],
      &self.levels[1],
      &mut self.next_table_id,
    )
    .await?;

    if result.new_tables.is_empty() && result.old_tables.is_empty() {
      return Ok(false);
    }

    // Remove old tables from L0 and L1
    // 从 L0 和 L1 移除旧表
    self.remove_tables(0, &result.old_tables);
    self.remove_tables(1, &result.old_tables);

    // Load and add new tables to L1
    // 加载并添加新表到 L1
    for table_id in result.new_tables {
      let path = self.sstable_path(table_id);
      let reader = SSTableReader::open(path, table_id).await?;
      self.levels[1].add(reader);
    }

    // Delete old SSTable files
    // 删除旧 SSTable 文件
    for table_id in result.old_tables {
      let path = self.sstable_path(table_id);
      let _ = compio::fs::remove_file(&path).await;
    }

    Ok(true)
  }

  /// Run level compaction (L1+ to next level)
  /// 运行层级压缩（L1+ 到下一层级）
  ///
  /// Compacts tables from src_level to dst_level.
  /// 将表从 src_level 压缩到 dst_level。
  pub async fn compact_level(&mut self, src_level_idx: usize) -> Result<bool> {
    if src_level_idx == 0 {
      return self.compact_l0().await;
    }

    if src_level_idx >= self.levels.len() || self.levels[src_level_idx].is_empty() {
      return Ok(false);
    }

    let dst_level_idx = src_level_idx + 1;

    // Ensure destination level exists
    // 确保目标层级存在
    while self.levels.len() <= dst_level_idx {
      self.levels.push(Level::new(self.levels.len()));
    }

    // Check if this is the last level (skip tombstones)
    // 检查是否是最后一层（跳过删除标记）
    let is_last_level = dst_level_idx >= self.levels.len() - 1;

    let result = compact_level(
      &self.dir,
      &self.levels[src_level_idx],
      &self.levels[dst_level_idx],
      &mut self.next_table_id,
      is_last_level,
    )
    .await?;

    if result.new_tables.is_empty() && result.old_tables.is_empty() {
      return Ok(false);
    }

    // Remove old tables
    // 移除旧表
    self.remove_tables(src_level_idx, &result.old_tables);
    self.remove_tables(dst_level_idx, &result.old_tables);

    // Load and add new tables
    // 加载并添加新表
    for table_id in result.new_tables {
      let path = self.sstable_path(table_id);
      let reader = SSTableReader::open(path, table_id).await?;
      self.levels[dst_level_idx].add(reader);
    }

    // Delete old SSTable files
    // 删除旧 SSTable 文件
    for table_id in result.old_tables {
      let path = self.sstable_path(table_id);
      let _ = compio::fs::remove_file(&path).await;
    }

    Ok(true)
  }

  /// Run compaction if needed
  /// 如果需要则运行压缩
  ///
  /// Returns true if compaction was performed.
  /// 如果执行了压缩则返回 true。
  pub async fn maybe_compact(&mut self) -> Result<bool> {
    if let Some(level_idx) = self.needs_compaction() {
      self.compact_level(level_idx).await
    } else {
      Ok(false)
    }
  }

  /// Remove tables from a level by ID
  /// 按 ID 从层级移除表
  fn remove_tables(&mut self, level_idx: usize, table_ids: &[u64]) {
    if level_idx >= self.levels.len() {
      return;
    }
    self.levels[level_idx]
      .tables
      .retain(|t| !table_ids.contains(&t.meta().id));
  }

  /// Get next table ID (for manifest)
  /// 获取下一个表 ID（用于清单）
  #[inline]
  pub fn next_table_id(&self) -> u64 {
    self.next_table_id
  }

  /// Set next table ID (for recovery)
  /// 设置下一个表 ID（用于恢复）
  #[inline]
  pub fn set_next_table_id(&mut self, id: u64) {
    self.next_table_id = id;
  }

  /// Get mutable levels reference
  /// 获取可变层级引用
  #[inline]
  pub fn levels_mut(&mut self) -> &mut Vec<Level> {
    &mut self.levels
  }
}

/// Convert bounds to concrete keys for SSTable range query
/// 将边界转换为 SSTable 范围查询的具体键
fn bounds_to_keys(start: Bound<&[u8]>, end: Bound<&[u8]>) -> (Vec<u8>, Vec<u8>) {
  let start_key = match start {
    Bound::Included(k) | Bound::Excluded(k) => k.to_vec(),
    Bound::Unbounded => vec![],
  };
  let end_key = match end {
    Bound::Included(k) | Bound::Excluded(k) => k.to_vec(),
    Bound::Unbounded => vec![0xff; 256], // Max key
  };
  (start_key, end_key)
}

/// Calculate the exclusive end bound for a prefix
/// 计算前缀的排他结束边界
///
/// Returns None if prefix is all 0xff (no upper bound).
/// 如果前缀全是 0xff 则返回 None（无上界）。
fn prefix_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
  let mut end = prefix.to_vec();

  // Increment the last byte, handling overflow
  // 增加最后一个字节，处理溢出
  while let Some(last) = end.pop() {
    if last < 0xff {
      end.push(last + 1);
      return Some(end);
    }
    // Overflow, continue to previous byte
    // 溢出，继续到前一个字节
  }

  // All bytes were 0xff, no upper bound
  // 所有字节都是 0xff，无上界
  None
}
