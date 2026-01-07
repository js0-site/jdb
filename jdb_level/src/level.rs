//! Single level with PGM index
//! 带 PGM 索引的单层

use std::ops::Bound;

use jdb_base::{SortedVec, table::Meta};
use jdb_pgm::{Pgm, key_to_u64};

/// PGM epsilon for table lookup
/// 表查找的 PGM 误差
const PGM_EPSILON: usize = 4;

/// Minimum number of tables to enable PGM index
/// 启用 PGM 索引的最小表数量
const PGM_MIN_THRESHOLD: usize = 16;

/// Get start key from bound
/// 从边界获取起始键
#[inline]
fn bound_start(b: Bound<&[u8]>) -> Option<&[u8]> {
  match b {
    Bound::Included(k) | Bound::Excluded(k) => Some(k),
    Bound::Unbounded => None,
  }
}

/// Single level with PGM index
/// 带 PGM 索引的单层
pub struct Level<T> {
  n: u8,
  tables: SortedVec<T>,
  size: u64,
  pgm: Option<Pgm<u64>>,
  pgm_dirty: bool,
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
  pub fn drain(&mut self, indices: &[usize]) -> Vec<T> {
    let items = self.tables.drain_indices(indices);
    for m in &items {
      self.size -= m.size();
    }
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
  }

  /// Iterate overlapping table indices
  /// 迭代重叠的表索引
  pub fn overlapping<'a>(
    &'a self,
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
  ) -> impl Iterator<Item = usize> + 'a {
    let is_l1_plus = self.n > 0;
    let begin = if is_l1_plus {
      bound_start(start).map_or(0, |k| self.tables.partition_point(|t| t.max_key() < k))
    } else {
      0
    };

    self.tables[begin..]
      .iter()
      .enumerate()
      .take_while(move |(_, t)| {
        if !is_l1_plus {
          return true;
        }
        match end {
          Bound::Included(k) => t.min_key() <= k,
          Bound::Excluded(k) => t.min_key() < k,
          Bound::Unbounded => true,
        }
      })
      .filter(move |(_, t)| t.overlaps(start, end))
      .map(move |(i, _)| begin + i)
  }

  /// Find table containing key (PGM-accelerated for L1+)
  /// 查找包含键的表（L1+ 使用 PGM 加速）
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
    let mut keys = Vec::with_capacity(self.len());
    keys.extend(self.iter().map(|t| key_to_u64(t.min_key())));
    self.pgm = Pgm::new(&keys, PGM_EPSILON, false).ok();
  }
}
