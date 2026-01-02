//! Index - LSM-Tree index implementation
//! 索引 - LSM-Tree 索引实现
//!
//! Manages memtable, sealed memtables, and SSTable levels.
//! 管理内存表、密封内存表和 SSTable 层级。

use std::{ops::Bound, path::PathBuf};

use jdb_base::Pos;
use jdb_fs::{FileLru, fs_id::id_path};

use super::{
  Entry, Level, Memtable, MergeIter,
  compact::{compact_l0_to_l1, compact_level, needs_l0_compaction, needs_level_compaction},
};
use crate::{Conf, Result, SSTableWriter, TableInfo};

/// Default file cache capacity
/// 默认文件缓存容量
const FILE_CACHE_CAP: usize = 64;

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
  /// File handle cache
  /// 文件句柄缓存
  files: FileLru,
}

impl Index {
  /// Create new index
  /// 创建新索引
  pub fn new(dir: PathBuf, conf: Conf) -> Self {
    let sst_dir = dir.join("sst");
    Self {
      dir,
      memtable: Memtable::new(0),
      sealed: Vec::new(),
      levels: vec![Level::new(0)],
      next_table_id: 1,
      next_memtable_id: 1,
      conf,
      files: FileLru::new(sst_dir, FILE_CACHE_CAP),
    }
  }

  /// Get entry by key
  /// 按键获取条目
  ///
  /// Search order: memtable -> sealed memtables -> L0 -> L1 -> ...
  /// 搜索顺序：内存表 -> 密封内存表 -> L0 -> L1 -> ...
  pub async fn get(&mut self, key: &[u8]) -> Result<Option<Entry>> {
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
      if level.level == 0 {
        // L0: tables may overlap, search all (newest first)
        // L0：表可能重叠，搜索所有（最新的优先）
        for table in level.tables.iter().rev() {
          if !table.is_key_in_range(key) || !table.may_contain(key) {
            continue;
          }
          if let Some(entry) = table.get(key, &mut self.files).await? {
            return Ok(Some(entry));
          }
        }
      } else {
        // L1+: binary search
        // L1+：二分查找
        let idx = level
          .tables
          .partition_point(|t| t.meta().min_key.as_ref() <= key);
        if idx > 0 {
          let table = &level.tables[idx - 1];
          if table.is_key_in_range(key)
            && table.may_contain(key)
            && let Some(entry) = table.get(key, &mut self.files).await?
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

    let memtable = self.sealed.remove(0);
    if memtable.is_empty() {
      return Ok(None);
    }

    let sst_dir = self.sst_dir();
    compio::fs::create_dir_all(&sst_dir).await?;

    let table_id = self.next_table_id;
    self.next_table_id += 1;

    let path = self.sstable_path(table_id);
    let mut writer = SSTableWriter::new(path.clone(), table_id, memtable.len()).await?;

    for (key, entry) in memtable.iter() {
      writer.add(key, entry).await?;
    }

    let meta = writer.finish().await?;

    if meta.item_count == 0 {
      return Ok(None);
    }

    let info = TableInfo::load(&path, table_id).await?;
    if self.levels.is_empty() {
      self.levels.push(Level::new(0));
    }
    self.levels[0].add(info);

    Ok(Some(table_id))
  }

  /// Generate SSTable file path
  /// 生成 SSTable 文件路径
  fn sstable_path(&self, id: u64) -> PathBuf {
    id_path(&self.sst_dir(), id)
  }

  /// Get SSTable directory
  /// 获取 SSTable 目录
  #[inline]
  pub fn sst_dir(&self) -> PathBuf {
    self.dir.join("sst")
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
  pub async fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<MergeIter> {
    let mut sources: Vec<Vec<(Box<[u8]>, Entry)>> = Vec::new();

    // 1. Active memtable
    // 1. 活跃内存表
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

        let (start_key, end_key) = bounds_to_keys(start, end);
        let iter = table.range(&start_key, &end_key, &mut self.files).await?;
        let entries: Vec<_> = iter.collect();
        sources.push(entries);
      }
    }

    Ok(MergeIter::new(sources, true))
  }

  /// Iterate all entries with prefix
  /// 迭代所有带前缀的条目
  pub async fn prefix(&mut self, prefix: &[u8]) -> Result<MergeIter> {
    let end_bound = prefix_end_bound(prefix);

    let mut sources: Vec<Vec<(Box<[u8]>, Entry)>> = Vec::new();

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

    for sealed in self.sealed.iter().rev() {
      let entries: Vec<_> = sealed
        .range(start, end)
        .map(|(k, e)| (k.into(), *e))
        .collect();
      sources.push(entries);
    }

    let start_key = prefix.to_vec();
    let end_key = end_bound.clone().unwrap_or_else(|| vec![0xff; 256]);

    for level in &self.levels {
      for table in level.tables.iter().rev() {
        let meta = table.meta();
        if meta.max_key.as_ref() < prefix {
          continue;
        }
        if let Some(ref end_vec) = end_bound
          && meta.min_key.as_ref() >= end_vec.as_slice()
        {
          continue;
        }

        let iter = table.range(&start_key, &end_key, &mut self.files).await?;
        let entries: Vec<_> = iter.collect();
        sources.push(entries);
      }
    }

    Ok(MergeIter::new(sources, true))
  }

  /// Iterate all entries
  /// 迭代所有条目
  pub async fn iter(&mut self) -> Result<MergeIter> {
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
    if self.needs_l0_compaction() {
      return Some(0);
    }

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
  pub async fn compact_l0(&mut self) -> Result<bool> {
    if self.levels.is_empty() || self.levels[0].is_empty() {
      return Ok(false);
    }

    while self.levels.len() < 2 {
      self.levels.push(Level::new(self.levels.len()));
    }

    let result = compact_l0_to_l1(
      &self.sst_dir(),
      &self.levels[0],
      &self.levels[1],
      &mut self.next_table_id,
      &mut self.files,
    )
    .await?;

    if result.new_tables.is_empty() && result.old_tables.is_empty() {
      return Ok(false);
    }

    self.remove_tables(0, &result.old_tables);
    self.remove_tables(1, &result.old_tables);

    for table_id in result.new_tables {
      let path = self.sstable_path(table_id);
      let info = TableInfo::load(&path, table_id).await?;
      self.levels[1].add(info);
    }

    for table_id in result.old_tables {
      self.files.rm(table_id);
      let path = self.sstable_path(table_id);
      let _ = compio::fs::remove_file(&path).await;
    }

    Ok(true)
  }

  /// Run level compaction (L1+ to next level)
  /// 运行层级压缩（L1+ 到下一层级）
  pub async fn compact_level(&mut self, src_level_idx: usize) -> Result<bool> {
    if src_level_idx == 0 {
      return self.compact_l0().await;
    }

    if src_level_idx >= self.levels.len() || self.levels[src_level_idx].is_empty() {
      return Ok(false);
    }

    let dst_level_idx = src_level_idx + 1;

    while self.levels.len() <= dst_level_idx {
      self.levels.push(Level::new(self.levels.len()));
    }

    let is_last_level = dst_level_idx >= self.levels.len() - 1;

    let result = compact_level(
      &self.sst_dir(),
      &self.levels[src_level_idx],
      &self.levels[dst_level_idx],
      &mut self.next_table_id,
      is_last_level,
      &mut self.files,
    )
    .await?;

    if result.new_tables.is_empty() && result.old_tables.is_empty() {
      return Ok(false);
    }

    self.remove_tables(src_level_idx, &result.old_tables);
    self.remove_tables(dst_level_idx, &result.old_tables);

    for table_id in result.new_tables {
      let path = self.sstable_path(table_id);
      let info = TableInfo::load(&path, table_id).await?;
      self.levels[dst_level_idx].add(info);
    }

    for table_id in result.old_tables {
      self.files.rm(table_id);
      let path = self.sstable_path(table_id);
      let _ = compio::fs::remove_file(&path).await;
    }

    Ok(true)
  }

  /// Run compaction if needed
  /// 如果需要则运行压缩
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

  /// Get file cache reference
  /// 获取文件缓存引用
  #[inline]
  pub fn files(&mut self) -> &mut FileLru {
    &mut self.files
  }
}

/// Convert bounds to concrete keys
/// 将边界转换为具体键
fn bounds_to_keys(start: Bound<&[u8]>, end: Bound<&[u8]>) -> (Vec<u8>, Vec<u8>) {
  let start_key = match start {
    Bound::Included(k) | Bound::Excluded(k) => k.to_vec(),
    Bound::Unbounded => vec![],
  };
  let end_key = match end {
    Bound::Included(k) | Bound::Excluded(k) => k.to_vec(),
    Bound::Unbounded => vec![0xff; 256],
  };
  (start_key, end_key)
}

/// Calculate the exclusive end bound for a prefix
/// 计算前缀的排他结束边界
fn prefix_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
  let mut end = prefix.to_vec();

  while let Some(last) = end.pop() {
    if last < 0xff {
      end.push(last + 1);
      return Some(end);
    }
  }

  None
}
