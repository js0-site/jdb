//! SSTable read manager
//! SSTable 读取管理器

use std::{
  cell::RefCell,
  ops::Bound,
  path::{Path, PathBuf},
  rc::Rc,
};

use futures::Stream;
use jdb_base::{
  Pos,
  table::{Kv, SsTable},
};
use jdb_fs::FileLru;

use crate::{
  Result, TableInfo, TableMeta,
  load::load,
  stream::{MultiAsc, MultiDesc, asc_stream, desc_stream, filter_tables},
};

type Lru = Rc<RefCell<FileLru>>;

/// SSTable read manager, owns FileLru and all tables
/// SSTable 读取管理器，持有 FileLru 和所有表
pub struct Read {
  lru: Lru,
  tables: Vec<TableInfo>,
  dir: PathBuf,
}

impl Read {
  /// Create empty manager
  /// 创建空管理器
  #[inline]
  pub fn new(dir: &Path, lru_cap: usize) -> Self {
    Self {
      lru: Rc::new(RefCell::new(FileLru::new(dir, lru_cap))),
      tables: Vec::new(),
      dir: dir.to_path_buf(),
    }
  }

  /// Load all SSTables from directory
  /// 从目录加载所有 SSTable
  pub async fn load(dir: &Path, lru_cap: usize) -> Result<Self> {
    let tables = load(dir).await?;
    Ok(Self {
      lru: Rc::new(RefCell::new(FileLru::new(dir, lru_cap))),
      tables,
      dir: dir.to_path_buf(),
    })
  }

  /// Get directory path
  /// 获取目录路径
  #[inline]
  pub fn dir(&self) -> &Path {
    &self.dir
  }

  /// Add a table (maintains id asc order)
  /// 添加表（保持 id 升序）
  pub fn add(&mut self, info: TableInfo) {
    let id = info.meta().id;
    // Fast path: new table is newest (most common case)
    // 快速路径：新表是最新的（最常见情况）
    if self.tables.last().is_none_or(|t| t.meta().id < id) {
      self.tables.push(info);
    } else {
      let idx = self.tables.partition_point(|t| t.meta().id < id);
      self.tables.insert(idx, info);
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

  /// Get table metadata list
  /// 获取表元数据列表
  pub fn metas(&self) -> impl Iterator<Item = &TableMeta> {
    self.tables.iter().map(|t| t.meta())
  }

  /// Get table by index
  /// 按索引获取表
  #[inline]
  pub fn get_idx(&self, idx: usize) -> Option<&TableInfo> {
    self.tables.get(idx)
  }

  /// Get table by id (binary search, tables sorted by id asc)
  /// 按 id 获取表（二分查找，表按 id 升序）
  #[inline]
  pub fn get_id(&self, id: u64) -> Option<&TableInfo> {
    let idx = self.tables.partition_point(|t| t.meta().id < id);
    self.tables.get(idx).filter(|t| t.meta().id == id)
  }

  /// Get table indices that overlap with range
  /// 获取与范围重叠的表索引
  #[inline]
  pub fn tables_in_range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<usize> {
    filter_tables(&self.tables, start, end)
  }

  /// Range query on single table by index
  /// 按索引对单表进行范围查询
  pub fn range_table(
    &self,
    idx: usize,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl Stream<Item = Kv> + '_ {
    let info = &self.tables[idx];
    asc_stream(info, Rc::clone(&self.lru), start, end)
  }

  /// Reverse range query on single table by index
  /// 按索引对单表进行反向范围查询
  pub fn rev_range_table(
    &self,
    idx: usize,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl Stream<Item = Kv> + '_ {
    let info = &self.tables[idx];
    desc_stream(info, Rc::clone(&self.lru), start, end)
  }

  /// Iterate single table by index
  /// 按索引遍历单表
  #[inline]
  pub fn iter_table(&self, idx: usize) -> impl Stream<Item = Kv> + '_ {
    self.range_table(idx, Bound::Unbounded, Bound::Unbounded)
  }

  /// Reverse iterate single table by index
  /// 按索引反向遍历单表
  #[inline]
  pub fn rev_iter_table(&self, idx: usize) -> impl Stream<Item = Kv> + '_ {
    self.rev_range_table(idx, Bound::Unbounded, Bound::Unbounded)
  }
}

impl SsTable for Read {
  type RangeStream<'a> = MultiAsc<'a>;
  type RevStream<'a> = MultiDesc<'a>;

  /// Get value position by key (search all tables, newest first)
  /// 按键获取值位置（搜索所有表，最新优先）
  #[allow(clippy::await_holding_refcell_ref)]
  async fn get(&mut self, key: &[u8]) -> Option<Pos> {
    for info in self.tables.iter().rev() {
      if !info.is_key_in_range(key) || !info.may_contain(key) {
        continue;
      }
      let mut lru = self.lru.borrow_mut();
      if let Ok(Some(pos)) = info.get_pos(key, &mut lru).await {
        return Some(pos);
      }
    }
    None
  }

  /// Merge range query across all tables (dedup by key, newest wins)
  /// 跨所有表的合并范围查询（按键去重，最新优先）
  fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RangeStream<'_> {
    MultiAsc::new(&self.tables, Rc::clone(&self.lru), start, end)
  }

  /// Merge reverse range query across all tables
  /// 跨所有表的合并反向范围查询
  fn rev_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self::RevStream<'_> {
    MultiDesc::new(&self.tables, Rc::clone(&self.lru), start, end)
  }
}
