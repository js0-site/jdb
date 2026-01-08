//! Single level with PGM index
//! 带 PGM 索引的单层

use std::{ops::Bound, rc::Rc};

use either::Either;
use jdb_base::{SortedVec, sst::Meta};
use jdb_pgm::{Pgm, key_to_u64};

/// PGM epsilon for table lookup
/// 表查找的 PGM 误差
const PGM_EPSILON: usize = 8;

/// Minimum number of tables to enable PGM index
/// 启用 PGM 索引的最小表数量
const PGM_MIN_THRESHOLD: usize = 32;

/// Single level with PGM index (stores Rc<T> for snapshot support)
/// 带 PGM 索引的单层（存储 Rc<T> 以支持快照）
pub struct Level<T> {
  n: u8,
  tables: SortedVec<Rc<T>>,
  size: u64,
  pgm: Option<Pgm<u64>>,
  pgm_dirty: bool,
  /// Buffer for PGM keys to avoid frequent allocations
  /// PGM 键缓存，避免频繁分配
  keys_buf: Vec<u64>,
  /// Cursor for round-robin compaction picking
  /// 轮询压缩的游标（记录上次压缩到的 max_key）
  compact_cursor: Vec<u8>,
}

impl<T: AsRef<Meta>> Level<T> {
  #[inline]
  pub fn new(n: u8) -> Self {
    Self {
      n,
      tables: SortedVec::new(),
      size: 0,
      pgm: None,
      pgm_dirty: false,
      keys_buf: Vec::new(),
      compact_cursor: Vec::new(),
    }
  }

  #[inline]
  fn meta(t: &T) -> &Meta {
    t.as_ref()
  }

  #[inline]
  pub fn n(&self) -> u8 {
    self.n
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.tables.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.tables.is_empty()
  }

  #[inline]
  pub fn is_l0(&self) -> bool {
    self.n == 0
  }

  #[inline]
  pub fn size(&self) -> u64 {
    self.size
  }

  #[inline]
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
    self.tables.iter().map(|rc| rc.as_ref())
  }

  #[inline]
  pub fn get(&self, idx: usize) -> Option<&T> {
    self.tables.get(idx).map(|rc| rc.as_ref())
  }

  /// Get Rc reference by index (for snapshot)
  /// 按索引获取 Rc 引用（用于快照）
  #[inline]
  pub fn get_rc(&self, idx: usize) -> Option<Rc<T>> {
    self.tables.get(idx).cloned()
  }

  /// Iterate Rc references (for snapshot)
  /// 迭代 Rc 引用（用于快照）
  pub fn iter_rc(&self) -> impl Iterator<Item = Rc<T>> + '_ {
    self.tables.iter().cloned()
  }

  /// Add table (sorted by id for L0, by min_key for L1+)
  /// 添加表（L0 按 id 排序，L1+ 按 min_key 排序）
  pub fn add(&mut self, item: T) {
    self.add_rc(Rc::new(item));
  }

  /// Add table wrapped in Rc (sorted by id for L0, by min_key for L1+)
  /// 添加 Rc 包装的表（L0 按 id 排序，L1+ 按 min_key 排序）
  pub(crate) fn add_rc(&mut self, item: Rc<T>) {
    self.size += Self::meta(&item).file_size;
    if self.n == 0 {
      self
        .tables
        .push(item, |a, b| Self::meta(a).id.cmp(&Self::meta(b).id));
    } else {
      self.tables.push(item, |a, b| {
        Self::meta(a).min_key.cmp(&Self::meta(b).min_key)
      });
      self.pgm_dirty = true;
    }
  }

  /// Remove table by id, returns the Rc<T>
  /// 按 id 移除表，返回 Rc<T>
  pub fn rm(&mut self, id: u64) -> Option<Rc<T>> {
    let item = if self.n == 0 {
      self.tables.rm(&id, |t| Self::meta(t).id)
    } else {
      self
        .tables
        .iter()
        .position(|t| Self::meta(t).id == id)
        .map(|i| self.tables.remove(i))
    };
    if let Some(ref m) = item {
      self.size -= Self::meta(m).file_size;
      if self.n > 0 {
        self.pgm_dirty = true;
      }
    }
    item
  }

  #[inline]
  pub fn find(&self, id: u64) -> Option<usize> {
    if self.n == 0 {
      self.tables.find(&id, |t| Self::meta(t).id)
    } else {
      self.tables.iter().position(|t| Self::meta(t).id == id)
    }
  }

  /// Drain tables by sorted indices, returns Rc<T>
  /// 按已排序索引排出表，返回 Rc<T>
  #[inline]
  pub fn drain(&mut self, indices: &[usize]) -> Vec<Rc<T>> {
    let items = self.tables.drain_indices(indices);
    self.size -= items.iter().map(|m| Self::meta(m).file_size).sum::<u64>();
    if !items.is_empty() && self.n > 0 {
      self.pgm_dirty = true;
    }
    items
  }

  /// Clear all tables
  /// 清空所有表
  pub fn clear(&mut self) {
    self.tables.clear();
    self.size = 0;
    self.pgm = None;
    self.pgm_dirty = false;
    self.keys_buf.clear();
    self.compact_cursor.clear();
  }

  /// Iterate overlapping table indices
  /// 迭代重叠的表索引
  pub fn overlapping<'a>(
    &'a self,
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
  ) -> impl Iterator<Item = usize> + 'a {
    if self.n > 0 {
      let start_idx = match start {
        Bound::Included(k) | Bound::Excluded(k) => self
          .tables
          .partition_point(|t| Self::meta(t).max_key.as_ref() < k),
        Bound::Unbounded => 0,
      };

      Either::Left(
        self.tables[start_idx..]
          .iter()
          .enumerate()
          .take_while(move |(_, t)| match end {
            Bound::Included(k) => Self::meta(t).min_key.as_ref() <= k,
            Bound::Excluded(k) => Self::meta(t).min_key.as_ref() < k,
            Bound::Unbounded => true,
          })
          .filter(move |(_, t)| Self::meta(t).overlaps(start, end))
          .map(move |(i, _)| start_idx + i),
      )
    } else {
      Either::Right(
        self
          .tables
          .iter()
          .enumerate()
          .filter(move |(_, t)| Self::meta(t).overlaps(start, end))
          .map(|(i, _)| i),
      )
    }
  }

  /// Find table containing key (PGM-accelerated for L1+)
  /// 查找包含键的表（L1+ 使用 PGM 加速）
  #[inline]
  pub fn find_table(&mut self, key: &[u8]) -> Option<usize> {
    if self.is_l0() {
      return self
        .tables
        .iter()
        .enumerate()
        .rev()
        .find(|(_, t)| Self::meta(t).contains(key))
        .map(|(i, _)| i);
    }

    if let (Some(first), Some(last)) = (self.tables.first(), self.tables.last())
      && (key < Self::meta(first).min_key.as_ref() || key > Self::meta(last).max_key.as_ref())
    {
      return None;
    }

    if self.pgm_dirty {
      self.rebuild_pgm();
      self.pgm_dirty = false;
    }

    if let Some(ref pgm) = self.pgm {
      let idx = pgm.find(key, |i| self.get(i).map(|t| Self::meta(t).min_key.as_ref()));

      if idx > 0
        && self
          .tables
          .get(idx - 1)
          .is_some_and(|t| Self::meta(t).contains(key))
      {
        return Some(idx - 1);
      }
      if self
        .tables
        .get(idx)
        .is_some_and(|t| Self::meta(t).contains(key))
      {
        return Some(idx);
      }
      None
    } else {
      let idx = self
        .tables
        .partition_point(|t| Self::meta(t).max_key.as_ref() < key);
      if idx < self.tables.len() && Self::meta(&self.tables[idx]).contains(key) {
        Some(idx)
      } else {
        None
      }
    }
  }

  /// Rebuild PGM index
  /// 重建 PGM 索引
  fn rebuild_pgm(&mut self) {
    if self.len() < PGM_MIN_THRESHOLD {
      self.pgm = None;
      return;
    }
    self.keys_buf.clear();
    self.keys_buf.extend(
      self
        .tables
        .iter()
        .map(|t| key_to_u64(&Self::meta(t).min_key)),
    );
    self.pgm = Pgm::new(&self.keys_buf, PGM_EPSILON, false).ok();
  }

  /// Check if table has no overlap with this level (for trivial move)
  /// 检查表与本层是否无重叠（用于 trivial move）
  #[inline]
  pub fn no_overlap(&self, min_key: &[u8], max_key: &[u8]) -> bool {
    if self.is_empty() {
      return true;
    }
    if self.n == 0 {
      return false;
    }
    let start = Bound::Included(min_key);
    let end = Bound::Included(max_key);
    self.overlapping(start, end).next().is_none()
  }

  /// Check if all tables have no overlap with this level
  /// 检查所有表与本层是否都无重叠
  pub fn no_overlap_all<'a>(&self, mut tables: impl Iterator<Item = &'a T>) -> bool
  where
    T: 'a,
  {
    if self.is_empty() {
      return true;
    }
    if self.n == 0 {
      return false;
    }
    tables.all(|t| {
      let m = Self::meta(t);
      self.no_overlap(&m.min_key, &m.max_key)
    })
  }

  /// Calculate total size of overlapping tables
  /// 计算重叠表的总大小
  #[inline]
  pub fn overlapping_size(&self, min_key: &[u8], max_key: &[u8]) -> u64 {
    if self.is_empty() {
      return 0;
    }
    let start = Bound::Included(min_key);
    let end = Bound::Included(max_key);
    self
      .overlapping(start, end)
      .map(|i| Self::meta(&self.tables[i]).file_size)
      .sum()
  }

  /// Check if overlapping size exceeds limit (early exit optimization)
  /// 检查重叠大小是否超过限制（提前退出优化）
  #[inline]
  pub fn overlapping_exceeds(&self, min_key: &[u8], max_key: &[u8], limit: u64) -> bool {
    if self.is_empty() {
      return false;
    }
    let start = Bound::Included(min_key);
    let end = Bound::Included(max_key);
    let mut total = 0u64;
    for i in self.overlapping(start, end) {
      total += Self::meta(&self.tables[i]).file_size;
      if total > limit {
        return true;
      }
    }
    false
  }

  /// Pick file for compaction (Round-Robin with compensated size)
  /// 挑选用于压缩的文件（轮询 + 补偿大小优先）
  pub fn pick_file(&mut self) -> Option<usize> {
    if self.tables.is_empty() {
      return None;
    }

    if self.n == 0 {
      return self
        .tables
        .iter()
        .enumerate()
        .max_by_key(|(_, t)| Self::meta(t).compensated_size())
        .map(|(i, _)| i);
    }

    let start = self
      .tables
      .partition_point(|t| Self::meta(t).max_key.as_ref() <= self.compact_cursor.as_slice());

    let idx = if start < self.tables.len() { start } else { 0 };

    self.compact_cursor.clear();
    self
      .compact_cursor
      .extend_from_slice(&Self::meta(&self.tables[idx]).max_key);

    Some(idx)
  }

  /// Pick files for L0 compaction (all overlapping files)
  /// 挑选 L0 压缩文件（所有重叠的文件）
  pub fn pick_l0_files(&self, seed_idx: usize) -> Vec<usize> {
    if self.n != 0 || seed_idx >= self.tables.len() {
      return Vec::new();
    }

    let seed = Self::meta(&self.tables[seed_idx]);
    let mut min_key: &[u8] = &seed.min_key;
    let mut max_key: &[u8] = &seed.max_key;

    let mut picked = vec![false; self.tables.len()];
    picked[seed_idx] = true;

    loop {
      let mut expanded = false;
      for (i, t) in self.tables.iter().enumerate() {
        if picked[i] {
          continue;
        }
        let m = Self::meta(t);
        if m.overlaps(Bound::Included(min_key), Bound::Included(max_key)) {
          if m.min_key.as_ref() < min_key {
            min_key = &m.min_key;
          }
          if m.max_key.as_ref() > max_key {
            max_key = &m.max_key;
          }
          picked[i] = true;
          expanded = true;
        }
      }
      if !expanded {
        break;
      }
    }

    picked
      .into_iter()
      .enumerate()
      .filter(|(_, b)| *b)
      .map(|(i, _)| i)
      .collect()
  }
}
