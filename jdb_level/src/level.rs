//! Single level with PGM index
//! 带 PGM 索引的单层

use std::{ops::Bound, rc::Rc};

use either::Either;
use jdb_base::{SortedVec, table::Meta};
use jdb_pgm::{Pgm, key_to_u64};

/// PGM epsilon for table lookup
/// 表查找的 PGM 误差
const PGM_EPSILON: usize = 8;

/// Minimum number of tables to enable PGM index
/// 启用 PGM 索引的最小表数量
const PGM_MIN_THRESHOLD: usize = 32;

/// Single level with PGM index
/// 带 PGM 索引的单层
pub struct Level<T> {
  n: u8,
  tables: SortedVec<T>,
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

impl<T: Meta> Level<T> {
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
  pub fn iter(&self) -> std::slice::Iter<'_, T> {
    self.tables.iter()
  }

  #[inline]
  pub fn get(&self, idx: usize) -> Option<&T> {
    self.tables.get(idx)
  }

  /// Iterate Rc references (for snapshot)
  /// 迭代 Rc 引用（用于快照）
  pub fn iter_rc(&self) -> impl Iterator<Item = Rc<T>> + '_
  where
    T: Clone,
  {
    self.tables.iter().map(|t| Rc::new(t.clone()))
  }

  /// Add table (sorted by id for L0, by min_key for L1+)
  /// 添加表（L0 按 id 排序，L1+ 按 min_key 排序）
  pub fn add(&mut self, item: T) {
    self.size += item.size();
    if self.n == 0 {
      self.tables.push(item, |a, b| a.id().cmp(&b.id()));
    } else {
      self.tables.push(item, |a, b| a.min_key().cmp(b.min_key()));
      self.pgm_dirty = true;
    }
  }

  /// Remove table by id
  /// 按 id 移除表
  pub fn rm(&mut self, id: u64) -> Option<T> {
    let item = if self.n == 0 {
      self.tables.rm(&id, |t| t.id())
    } else {
      self
        .tables
        .iter()
        .position(|t| t.id() == id)
        .map(|i| self.tables.remove(i))
    };
    if let Some(ref m) = item {
      self.size -= m.size();
      if self.n > 0 {
        self.pgm_dirty = true;
      }
    }
    item
  }

  #[inline]
  pub fn find(&self, id: u64) -> Option<usize> {
    if self.n == 0 {
      self.tables.find(&id, |t| t.id())
    } else {
      self.tables.iter().position(|t| t.id() == id)
    }
  }

  /// Drain tables by sorted indices
  /// 按已排序索引排出表
  #[inline]
  pub fn drain(&mut self, indices: &[usize]) -> Vec<T> {
    let items = self.tables.drain_indices(indices);
    self.size -= items.iter().map(|m| m.size()).sum::<u64>();
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
    // Optimized for L1+: Binary search for start and end
    // L1+ 优化：二分查找起止点
    if self.n > 0 {
      let start_idx = match start {
        Bound::Included(k) | Bound::Excluded(k) => self.tables.partition_point(|t| t.max_key() < k),
        Bound::Unbounded => 0,
      };

      // L1+ tables are sorted by min_key and disjoint
      // L1+ 表按 min_key 排序且不重叠
      Either::Left(
        self.tables[start_idx..]
          .iter()
          .enumerate()
          .take_while(move |(_, t)| match end {
            Bound::Included(k) => t.min_key() <= k,
            Bound::Excluded(k) => t.min_key() < k,
            Bound::Unbounded => true,
          })
          .filter(move |(_, t)| t.overlaps(start, end))
          .map(move |(i, _)| start_idx + i),
      )
    } else {
      // L0: Linear scan (tables can overlap each other)
      // L0：线性扫描（表可能互相重叠）
      Either::Right(
        self
          .tables
          .iter()
          .enumerate()
          .filter(move |(_, t)| t.overlaps(start, end))
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
        .find(|(_, t)| t.contains(key))
        .map(|(i, _)| i);
    }

    // L1+ Fast path: check global bounds first
    // L1+ 快速路径：先检查全局边界
    if let (Some(first), Some(last)) = (self.tables.first(), self.tables.last())
      && (key < first.min_key() || key > last.max_key())
    {
      return None;
    }

    if self.pgm_dirty {
      self.rebuild_pgm();
      self.pgm_dirty = false;
    }

    // Try PGM lookup first
    // 优先尝试 PGM 查找
    if let Some(ref pgm) = self.pgm {
      let idx = pgm.find(key, |i| self.get(i).map(|t| t.min_key()));

      // Check idx-1 (because PGM returns approximate position)
      // 检查 idx-1（因为 PGM 返回近似位置）
      if idx > 0 && self.tables.get(idx - 1).is_some_and(|t| t.contains(key)) {
        return Some(idx - 1);
      }
      if self.tables.get(idx).is_some_and(|t| t.contains(key)) {
        return Some(idx);
      }
      None
    } else {
      // Fallback to binary search for small levels or if PGM failed
      // 小层级或 PGM 失败时回退到二分查找
      let idx = self.tables.partition_point(|t| t.max_key() < key);
      if idx < self.tables.len() && self.tables[idx].contains(key) {
        Some(idx)
      } else {
        None
      }
    }
  }

  /// Rebuild PGM index
  /// 重建 PGM 索引
  fn rebuild_pgm(&mut self) {
    // Don't build PGM for small levels to save memory and time
    // 小层级不构建 PGM 以节省内存和时间
    if self.len() < PGM_MIN_THRESHOLD {
      self.pgm = None;
      return;
    }
    // Reuse keys_buf to reduce memory allocation
    // 复用 keys_buf 以减少内存分配
    self.keys_buf.clear();
    self
      .keys_buf
      .extend(self.tables.iter().map(|t| key_to_u64(t.min_key())));
    self.pgm = Pgm::new(&self.keys_buf, PGM_EPSILON, false).ok();
  }

  /// Check if table has no overlap with this level (for trivial move)
  /// 检查表与本层是否无重叠（用于 trivial move）
  #[inline]
  pub fn no_overlap(&self, min_key: &[u8], max_key: &[u8]) -> bool {
    if self.is_empty() {
      return true;
    }
    // L0 always has potential overlap (tables can overlap each other)
    // L0 总是可能有重叠（表之间可能重叠）
    if self.n == 0 {
      return false;
    }
    // L1+: binary search to check overlap
    // L1+：二分查找检查重叠
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
    tables.all(|t| self.no_overlap(t.min_key(), t.max_key()))
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
      .map(|i| self.tables[i].size())
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
      total += self.tables[i].size();
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

    // L0: pick file with highest compensated_size (most tombstones)
    // L0：选补偿大小最大的文件（删除标记最多）
    if self.n == 0 {
      return self
        .tables
        .iter()
        .enumerate()
        .max_by_key(|(_, t)| t.compensated_size())
        .map(|(i, _)| i);
    }

    // L1+: round-robin from cursor position
    // L1+：从游标位置轮询
    let start = self
      .tables
      .partition_point(|t| t.max_key() <= self.compact_cursor.as_slice());

    let idx = if start < self.tables.len() {
      start
    } else {
      0 // wrap around / 回绕
    };

    // Update cursor to current file's max_key
    // 更新游标为当前文件的 max_key
    self.compact_cursor.clear();
    self
      .compact_cursor
      .extend_from_slice(self.tables[idx].max_key());

    Some(idx)
  }

  /// Pick files for L0 compaction (all overlapping files)
  /// 挑选 L0 压缩文件（所有重叠的文件）
  pub fn pick_l0_files(&self, seed_idx: usize) -> Vec<usize> {
    if self.n != 0 || seed_idx >= self.tables.len() {
      return Vec::new();
    }

    let seed = &self.tables[seed_idx];
    // Use slices to avoid allocation
    // 使用切片引用避免内存分配
    let mut min_key = seed.min_key();
    let mut max_key = seed.max_key();

    // Use bitmap for O(1) checking instead of O(N) contains()
    // 使用位图进行 O(1) 检查，替代 O(N) 的 contains()
    let mut picked = vec![false; self.tables.len()];
    picked[seed_idx] = true;

    // Expand to include all overlapping L0 files
    // L0 count is small, so O(N^2) loop is acceptable (< 64 iter)
    // 扩展以包含所有重叠的 L0 文件
    // L0 数量小，O(N^2) 循环可接受（< 64 次迭代）
    loop {
      let mut expanded = false;
      for (i, t) in self.tables.iter().enumerate() {
        if picked[i] {
          continue;
        }
        // Check overlap using Meta::overlaps
        // 使用 Meta::overlaps 检查重叠
        if t.overlaps(Bound::Included(min_key), Bound::Included(max_key)) {
          if t.min_key() < min_key {
            min_key = t.min_key();
          }
          if t.max_key() > max_key {
            max_key = t.max_key();
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
