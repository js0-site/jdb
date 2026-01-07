//! SSTable read manager with level support
//! 支持层级的 SSTable 读取管理器

use std::{
  cell::RefCell,
  ops::Bound,
  path::{Path, PathBuf},
  rc::Rc,
};

use futures::Stream;
use jdb_base::{
  Pos,
  table::{Kv, Meta, SsTable},
};
use jdb_fs::FileLru;

use crate::{
  Level, Levels, Result, Table,
  level::new_levels,
  load::load,
  stream::{MultiAsc, MultiDesc, asc_stream, desc_stream},
};

type Lru = Rc<RefCell<FileLru>>;

/// SSTable read manager with level support
/// 支持层级的 SSTable 读取管理器
pub struct Read {
  lru: Lru,
  levels: Levels,
  dir: PathBuf,
}

impl Read {
  /// Create empty manager
  /// 创建空管理器
  #[inline]
  pub fn new(dir: &Path, lru_cap: usize) -> Self {
    Self {
      lru: Rc::new(RefCell::new(FileLru::new(dir, lru_cap))),
      levels: new_levels(),
      dir: dir.to_path_buf(),
    }
  }

  /// Load all SSTables from directory
  /// 从目录加载所有 SSTable
  pub async fn load(dir: &Path, lru_cap: usize) -> Result<Self> {
    let levels = load(dir).await?;
    Ok(Self {
      lru: Rc::new(RefCell::new(FileLru::new(dir, lru_cap))),
      levels,
      dir: dir.to_path_buf(),
    })
  }

  /// Get directory path
  /// 获取目录路径
  #[inline]
  pub fn dir(&self) -> &Path {
    &self.dir
  }

  /// Add table to L0
  /// 添加表到 L0
  pub fn add(&mut self, info: Table) {
    if let Some(l0) = self.levels.li.get_mut(0) {
      l0.add(info);
    }
  }

  /// Get level manager
  /// 获取层级管理器
  #[inline]
  pub fn levels(&self) -> &Levels {
    &self.levels
  }

  /// Get mutable level manager
  /// 获取可变层级管理器
  #[inline]
  pub fn levels_mut(&mut self) -> &mut Levels {
    &mut self.levels
  }

  /// Get level by number
  /// 按编号获取层级
  #[inline]
  pub fn level(&self, n: u8) -> Option<&Level> {
    self.levels.li.get(n as usize)
  }

  /// Get mutable level by number
  /// 按编号获取可变层级
  #[inline]
  pub fn level_mut(&mut self, n: u8) -> Option<&mut Level> {
    self.levels.li.get_mut(n as usize)
  }

  /// Total table count across all levels
  /// 所有层级的表总数
  #[inline]
  pub fn len(&self) -> usize {
    self.levels.table_count()
  }

  /// Check if empty
  /// 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.levels.table_count() == 0
  }

  /// Get table by level and index
  /// 按层级和索引获取表
  #[inline]
  pub fn get(&self, level: u8, idx: usize) -> Option<&Table> {
    self.levels.li.get(level as usize).and_then(|l| l.get(idx))
  }

  /// Collect overlapping tables for range query
  /// 收集范围查询的重叠表
  fn range_tables<'a>(&'a self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<&'a Table> {
    // Estimate: L0 tables + ~2 tables per L1+ level
    // 预估：L0 表数 + 每个 L1+ 层约 2 个表
    let l0_len = self.levels.li.first().map_or(0, |l| l.len());
    let cap = l0_len + (self.levels.max_level as usize) * 2;
    let mut result = Vec::with_capacity(cap.min(32));

    for level in self.levels.li.iter() {
      if level.is_l0() {
        // L0: all tables may overlap, add all (newest first)
        // L0：所有表可能重叠，全部添加（最新优先）
        result.extend(level.iter().rev());
      } else {
        // L1+: only add overlapping tables
        // L1+：只添加重叠的表
        for idx in level.overlapping(start, end) {
          if let Some(t) = level.get(idx) {
            result.push(t);
          }
        }
      }
    }
    result
  }

  /// Range query on single table
  /// 对单表进行范围查询
  pub fn range_table<'a>(
    &'a self,
    info: &'a Table,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl Stream<Item = Kv> + 'a {
    asc_stream(info, Rc::clone(&self.lru), start, end)
  }

  /// Reverse range query on single table
  /// 对单表进行反向范围查询
  pub fn rev_range_table<'a>(
    &'a self,
    info: &'a Table,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl Stream<Item = Kv> + 'a {
    desc_stream(info, Rc::clone(&self.lru), start, end)
  }
}

impl SsTable for Read {
  type RangeStream<'a> = MultiAsc<'a>;
  type RevStream<'a> = MultiDesc<'a>;

  /// Get value position by key (search levels: L0 newest first, then L1, L2...)
  /// 按键获取值位置（搜索层级：L0 最新优先，然后 L1, L2...）
  #[allow(clippy::await_holding_refcell_ref)]
  async fn get(&mut self, key: &[u8]) -> Option<Pos> {
    let mut lru = self.lru.borrow_mut();

    // Search L0 first (newest to oldest)
    // 先搜索 L0（从新到旧）
    // Bloom filter first (faster than range check for L0 with many tables)
    // 先检查布隆过滤器（对于 L0 多表场景比范围检查更快）
    if let Some(l0) = self.levels.li.first() {
      for info in l0.iter().rev() {
        if !info.may_contain(key) || !info.contains(key) {
          continue;
        }
        if let Ok(Some(pos)) = info.get_pos_unchecked(key, &mut lru).await {
          return Some(pos);
        }
      }
    }

    // Search L1+ (PGM + binary search within each level)
    // 搜索 L1+（每层内 PGM + 二分查找）
    for level_num in 1..=self.levels.max_level {
      if let Some(level) = self.levels.li.get_mut(level_num as usize)
        && let Some(idx) = level.find_table(key)
        && let Some(info) = level.get(idx)
        && info.may_contain(key)
        && let Ok(Some(pos)) = info.get_pos_unchecked(key, &mut lru).await
      {
        return Some(pos);
      }
    }

    None
  }

  /// Merge range query across all levels
  /// 跨所有层级的合并范围查询
  fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RangeStream<'_> {
    let tables = self.range_tables(start, end);
    MultiAsc::from_refs(tables, Rc::clone(&self.lru), start, end)
  }

  /// Merge reverse range query across all levels
  /// 跨所有层级的合并反向范围查询
  fn rev_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RevStream<'_> {
    let tables = self.range_tables(start, end);
    MultiDesc::from_refs(tables, Rc::clone(&self.lru), start, end)
  }
}
