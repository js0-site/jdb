//! SSTable read manager with level support
//! 支持层级的 SSTable 读取管理器

use std::{
  cell::RefCell,
  collections::HashMap,
  ops::Bound,
  path::{Path, PathBuf},
  pin::Pin,
  rc::Rc,
};

use futures_core::Stream;
use jdb_base::{
  Kv, Pos,
  sst::{Asc, Desc},
};
use jdb_ckp::Ckp;
use jdb_fs::FileLru;
use jdb_sst::{Conf, Table};
use log::error;

use crate::{
  Compactor, Error, Handle, Levels, Result,
  load::load,
  multi::{Multi, new_asc_from_refs, new_desc_from_refs},
};

type Lru = Rc<RefCell<FileLru>>;

/// SSTable read manager with level support
/// 支持层级的 SSTable 读取管理器
pub struct Read {
  lru: Lru,
  dir: Rc<PathBuf>,
  levels: Levels,
  handles: HashMap<u64, Handle>,
  conf: Vec<Conf>,
}

impl Read {
  /// Load all SSTables from directory
  /// 从目录加载所有 SSTable
  pub async fn load(dir: &Path, lru_cap: usize, ckp: Rc<RefCell<Ckp>>) -> Result<Self> {
    let lru = Rc::new(RefCell::new(FileLru::new(dir, lru_cap)));
    let (levels, handles) = load(dir, ckp, Rc::clone(&lru)).await?;
    Ok(Self {
      lru,
      dir: Rc::new(dir.to_path_buf()),
      levels,
      handles,
      conf: Vec::new(),
    })
  }

  /// Set write config for compaction
  /// 设置压缩写入配置
  #[inline]
  pub fn set_conf(&mut self, conf: Vec<Conf>) {
    self.conf = conf;
  }

  /// Execute one round of compaction
  /// 执行一轮压缩
  pub async fn compact(&mut self) -> Result<bool> {
    let mut compactor = Compactor::new(&self.dir, Rc::clone(&self.lru), &self.conf);
    self.levels.compact(&mut compactor).await.map_err(|e| {
      error!("compact: {e:?}");
      Error::Compact
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
  pub fn add(&mut self, table: Table) {
    let handle = Handle::new(table, Rc::clone(&self.dir), Rc::clone(&self.lru));
    handle.mark_rm();
    let meta = handle.meta().clone();
    let id = meta.id;
    self.handles.insert(id, handle);
    if let Some(l0) = self.levels.levels.get_mut(0) {
      l0.add(meta);
    }
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

  /// Collect overlapping tables for range query
  /// 收集范围查询的重叠表
  fn range_tables<'a>(&'a self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<&'a Table> {
    let l0_len = self.levels.levels.first().map_or(0, |l| l.len());
    let cap = l0_len + (self.levels.max_level() as usize) * 2;
    let mut result = Vec::with_capacity(cap.min(32));

    for level in self.levels.levels.iter() {
      if level.is_l0() {
        // L0: all tables may overlap, add all (newest first)
        // L0：所有表可能重叠，全部添加（最新优先）
        for meta in level.iter().rev() {
          if let Some(h) = self.handles.get(&meta.id) {
            result.push(h.table());
          }
        }
      } else {
        // L1+: only add overlapping tables
        // L1+：只添加重叠的表
        for idx in level.overlapping(start, end) {
          if let Some(meta) = level.get(idx)
            && let Some(h) = self.handles.get(&meta.id)
          {
            result.push(h.table());
          }
        }
      }
    }
    result
  }

  /// Get value position by key (search levels: L0 newest first, then L1, L2...)
  /// 按键获取值位置（搜索层级：L0 最新优先，然后 L1, L2...）
  #[allow(clippy::await_holding_refcell_ref)]
  pub async fn get(&mut self, key: &[u8]) -> Option<Pos> {
    let mut lru = self.lru.borrow_mut();

    // Search L0 first (newest to oldest)
    // 先搜索 L0（从新到旧）
    if let Some(l0) = self.levels.levels.first() {
      for meta in l0.iter().rev() {
        let Some(h) = self.handles.get(&meta.id) else {
          continue;
        };
        let table = h.table();
        if !table.may_contain(key) || !table.is_key_in_range(key) {
          continue;
        }
        if let Ok(Some(pos)) = table.get_pos_unchecked(key, &mut lru).await {
          return Some(pos);
        }
      }
    }

    // Search L1+ (PGM + binary search within each level)
    // 搜索 L1+（每层内 PGM + 二分查找）
    for level_num in 1..=self.levels.max_level() {
      if let Some(level) = self.levels.levels.get_mut(level_num as usize)
        && let Some(idx) = level.find_table(key)
        && let Some(meta) = level.get(idx)
        && let Some(h) = self.handles.get(&meta.id)
      {
        let table = h.table();
        if table.may_contain(key)
          && let Ok(Some(pos)) = table.get_pos_unchecked(key, &mut lru).await
        {
          return Some(pos);
        }
      }
    }

    None
  }

  /// Forward range query [start, end)
  /// 正向范围查询 [start, end)
  #[inline]
  pub fn range(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> Multi<'_, Asc, Pin<Box<impl Stream<Item = Kv> + '_>>> {
    let tables = self.range_tables(start, end);
    new_asc_from_refs(tables.into_iter(), Rc::clone(&self.lru), start, end)
  }

  /// Reverse range query (end, start]
  /// 反向范围查询 (end, start]
  #[inline]
  pub fn rev_range(
    &mut self,
    end: Bound<&[u8]>,
    start: Bound<&[u8]>,
  ) -> Multi<'_, Desc, Pin<Box<impl Stream<Item = Kv> + '_>>> {
    let tables = self.range_tables(start, end);
    new_desc_from_refs(tables.into_iter(), Rc::clone(&self.lru), start, end)
  }

  /// Iterate all entries ascending
  /// 升序迭代所有条目
  #[inline]
  pub fn iter(&mut self) -> Multi<'_, Asc, Pin<Box<impl Stream<Item = Kv> + '_>>> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate all entries descending
  /// 降序迭代所有条目
  #[inline]
  pub fn rev_iter(&mut self) -> Multi<'_, Desc, Pin<Box<impl Stream<Item = Kv> + '_>>> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }
}
