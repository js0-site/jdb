//! SSTable level with PGM-index for L1+
//! 带 PGM 索引的 SSTable 层级（L1+）

use std::ops::Bound;

use jdb_base::table::{Meta, level::Conf};
use jdb_pgm::{Pgm, key_to_u64};

use crate::Table;

/// PGM epsilon for table lookup (small for high precision)
/// 表查找的 PGM 误差（小值以获得高精度）
const PGM_EPSILON: usize = 4;

type BaseLevel = jdb_base::table::level::Level<Table>;
type BaseLevels = jdb_base::table::level::Levels<Table>;

/// Level with optional PGM-index (L1+ only)
/// 带可选 PGM 索引的层级（仅 L1+）
pub struct Level {
  inner: BaseLevel,
  /// PGM index for L1+ (None for L0 or empty level)
  /// L1+ 的 PGM 索引（L0 或空层级为 None）
  pgm: Option<Pgm<u64>>,
  /// PGM needs rebuild
  /// PGM 需要重建
  dirty: bool,
}

impl Level {
  #[inline]
  pub fn new(n: u8) -> Self {
    Self {
      inner: BaseLevel::new(n),
      pgm: None,
      dirty: false,
    }
  }

  #[inline]
  pub fn n(&self) -> u8 {
    self.inner.n()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.inner.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }

  #[inline]
  pub fn is_l0(&self) -> bool {
    self.inner.is_l0()
  }

  #[inline]
  pub fn size(&self) -> u64 {
    self.inner.size()
  }

  #[inline]
  pub fn iter(&self) -> std::slice::Iter<'_, Table> {
    self.inner.iter()
  }

  #[inline]
  pub fn get(&self, idx: usize) -> Option<&Table> {
    self.inner.get(idx)
  }

  /// Add table and mark PGM dirty for L1+
  /// 添加表并标记 L1+ 的 PGM 为脏
  pub fn add(&mut self, item: Table) {
    self.inner.add(item);
    if !self.is_l0() {
      self.dirty = true;
    }
  }

  /// Remove table by id and mark PGM dirty for L1+
  /// 按 id 移除表并标记 L1+ 的 PGM 为脏
  pub fn rm(&mut self, id: u64) -> Option<Table> {
    let item = self.inner.rm(id);
    if item.is_some() && !self.is_l0() {
      self.dirty = true;
    }
    item
  }

  #[inline]
  pub fn find(&self, id: u64) -> Option<usize> {
    self.inner.find(id)
  }

  /// Drain tables and mark PGM dirty for L1+
  /// 排出表并标记 L1+ 的 PGM 为脏
  pub fn drain(&mut self, indices: Vec<usize>) -> Vec<Table> {
    let items = self.inner.drain(indices);
    if !items.is_empty() && !self.is_l0() {
      self.dirty = true;
    }
    items
  }

  /// Clear all tables
  /// 清空所有表
  pub fn clear(&mut self) {
    self.inner.clear();
    self.pgm = None;
    self.dirty = false;
  }

  #[inline]
  pub fn overlapping<'a>(
    &'a self,
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
  ) -> impl Iterator<Item = usize> + 'a {
    self.inner.overlapping(start, end)
  }

  /// Find table containing key (PGM-accelerated for L1+)
  /// 查找包含键的表（L1+ 使用 PGM 加速）
  pub fn find_table(&mut self, key: &[u8]) -> Option<usize> {
    if self.is_l0() {
      return self.inner.find_table(key);
    }

    // Rebuild PGM if dirty
    // 如果脏则重建 PGM
    if self.dirty {
      self.rebuild_pgm();
      self.dirty = false;
    }

    // L1+: use PGM + binary search if available
    // L1+：如果有 PGM 则使用 PGM + 二分查找
    if let Some(ref pgm) = self.pgm {
      // PGM find: predict range then binary search by min_key bytes
      // PGM 查找：预测范围后按 min_key 字节二分
      let idx = pgm.find(key, |i| self.get(i).map(|t| t.min_key()));
      // idx is partition_point, check idx-1 and idx
      // idx 是分割点，检查 idx-1 和 idx
      if idx > 0
        && let Some(t) = self.get(idx - 1)
        && t.contains(key)
      {
        return Some(idx - 1);
      }
      if let Some(t) = self.get(idx)
        && t.contains(key)
      {
        return Some(idx);
      }
      None
    } else {
      self.inner.find_table(key)
    }
  }

  /// Rebuild PGM index from current tables
  /// 从当前表重建 PGM 索引
  fn rebuild_pgm(&mut self) {
    if self.len() < 2 {
      self.pgm = None;
      return;
    }

    // Build keys from min_key of each table
    // 从每个表的 min_key 构建键
    let keys: Vec<u64> = self.iter().map(|t| key_to_u64(t.min_key())).collect();

    self.pgm = Pgm::new(&keys, PGM_EPSILON, false).ok();
  }
}

/// Levels manager with PGM-indexed levels
/// 带 PGM 索引层级的层级管理器
pub struct Levels {
  pub li: Vec<Level>,
  pub max_level: u8,
  pub l0_limit: usize,
  pub l1_size: u64,
  pub size_ratio: u64,
}

impl Levels {
  pub fn new(conf: &[Conf]) -> Self {
    let base = BaseLevels::new(conf);
    let li = (0..=base.max_level).map(Level::new).collect();
    Self {
      li,
      max_level: base.max_level,
      l0_limit: base.l0_limit,
      l1_size: base.l1_size,
      size_ratio: base.size_ratio,
    }
  }

  #[inline]
  pub fn size_limit(&self, level: u8) -> u64 {
    if level == 0 {
      u64::MAX
    } else {
      self.l1_size * self.size_ratio.pow(level.saturating_sub(1) as u32)
    }
  }

  #[inline]
  pub fn needs_compaction(&self, level: u8) -> bool {
    self.li.get(level as usize).is_some_and(|l| {
      if level == 0 {
        l.len() >= self.l0_limit
      } else {
        l.size() > self.size_limit(level)
      }
    })
  }

  #[inline]
  pub fn next_compaction(&self) -> Option<u8> {
    (0..=self.max_level).find(|&level| self.needs_compaction(level))
  }

  #[inline]
  pub fn table_count(&self) -> usize {
    self.li.iter().map(|l| l.len()).sum()
  }

  #[inline]
  pub fn total_size(&self) -> u64 {
    self.li.iter().map(|l| l.size()).sum()
  }
}

#[inline]
pub fn new_levels() -> Levels {
  Levels::new(&[])
}

#[inline]
pub fn new_levels_conf(conf: &[Conf]) -> Levels {
  Levels::new(conf)
}
