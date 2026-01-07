//! Level - LSM-tree level abstraction
//! LSM-tree 层级抽象

use std::ops::Bound;

use super::Meta;
use crate::SortedVec;

/// Default configuration values
/// 默认配置值
pub mod default {
  pub const MAX_LEVEL: u8 = 7;
  pub const L0_LIMIT: usize = 4;
  pub const L1_SIZE: u64 = 8 * 1024 * 1024;
  pub const SIZE_RATIO: u64 = 8;
}

/// Level manager configuration
/// 层级管理器配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  MaxLevel(u8),
  L0Limit(usize),
  L1Size(u64),
  SizeRatio(u64),
}

/// Get start key from bound
/// 从边界获取起始键
#[inline]
fn bound_start(b: Bound<&[u8]>) -> Option<&[u8]> {
  match b {
    Bound::Included(k) | Bound::Excluded(k) => Some(k),
    Bound::Unbounded => None,
  }
}

/// LSM-tree level
/// LSM-tree 层级
pub struct Level<M> {
  n: u8,
  li: SortedVec<M>,
  size: u64,
}

impl<M> Default for Level<M> {
  fn default() -> Self {
    Self {
      n: 0,
      li: SortedVec::new(),
      size: 0,
    }
  }
}

impl<M: Meta> Level<M> {
  #[inline]
  pub fn new(n: u8) -> Self {
    Self {
      n,
      li: SortedVec::new(),
      size: 0,
    }
  }

  #[inline]
  pub fn n(&self) -> u8 {
    self.n
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.li.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.li.is_empty()
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
  pub fn iter(&self) -> std::slice::Iter<'_, M> {
    self.li.iter()
  }

  #[inline]
  pub fn get(&self, idx: usize) -> Option<&M> {
    self.li.get(idx)
  }

  /// Add table (sorted by id for L0, by min_key for L1+)
  /// 添加表（L0 按 id 排序，L1+ 按 min_key 排序）
  #[inline]
  pub fn add(&mut self, item: M) {
    self.size += item.size();
    if self.n == 0 {
      self.li.push(item, |a, b| a.id().cmp(&b.id()));
    } else {
      self.li.push(item, |a, b| a.min_key().cmp(b.min_key()));
    }
  }

  /// Remove table by id (binary search for L0)
  /// 按 id 移除表（L0 用二分查找）
  pub fn rm(&mut self, id: u64) -> Option<M> {
    let item = if self.n == 0 {
      self.li.rm(&id, |t| t.id())
    } else {
      self
        .li
        .iter()
        .position(|t| t.id() == id)
        .map(|i| self.li.remove(i))
    };
    if let Some(ref m) = item {
      self.size -= m.size();
    }
    item
  }

  /// Find table index by id (binary search for L0)
  /// 按 id 查找表索引（L0 用二分查找）
  #[inline]
  pub fn find(&self, id: u64) -> Option<usize> {
    if self.n == 0 {
      self.li.find(&id, |t| t.id())
    } else {
      self.li.iter().position(|t| t.id() == id)
    }
  }

  /// Drain tables by indices (must be sorted desc)
  /// 按索引排出表（必须降序排列）
  ///
  /// Input `indices` will be sorted ascending internally for O(N) efficiency.
  /// 输入 `indices` 将在内部升序排序以实现 O(N) 效率。
  pub fn drain(&mut self, mut indices: Vec<usize>) -> Vec<M> {
    indices.sort_unstable();
    let items = self.li.drain_indices(&indices);
    items.iter().for_each(|m| self.size -= m.size());
    items
  }

  /// Clear all tables
  /// 清空所有表
  #[inline]
  pub fn clear(&mut self) {
    self.li.clear();
    self.size = 0;
  }

  /// Iterate overlapping table indices (O(1) space)
  /// 迭代重叠的表索引（O(1) 空间）
  #[inline]
  pub fn overlapping<'a>(
    &'a self,
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
  ) -> impl Iterator<Item = usize> + 'a {
    // L1+: binary search to find start position
    // L1+：二分查找起始位置
    let is_l1_plus = self.n > 0;
    let begin = if is_l1_plus {
      bound_start(start).map_or(0, |k| {
        // Find first table where max_key >= start (predicate is max_key < start)
        // 找到第一个 max_key >= start 的表（谓词为 max_key < start）
        self.li.partition_point(|t| t.max_key() < k)
      })
    } else {
      0
    };

    self.li[begin..]
      .iter()
      .enumerate()
      .take_while(move |(_, t)| {
        // Early termination for L1+: stop when min_key > end
        // L1+ 提前终止：当 min_key > end 时停止
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

  /// Find table containing key
  /// 查找包含键的表
  pub fn find_table(&self, key: &[u8]) -> Option<usize> {
    if self.is_l0() {
      // L0: search newest first (reverse)
      // L0：从最新开始搜索（反向）
      self
        .li
        .iter()
        .enumerate()
        .rev()
        .find(|(_, t)| t.contains(key))
        .map(|(i, _)| i)
    } else {
      // L1+: binary search by max_key
      // L1+：按 max_key 二分查找
      let idx = self.li.partition_point(|t| t.max_key() < key);
      if idx < self.li.len() && self.li[idx].contains(key) {
        Some(idx)
      } else {
        None
      }
    }
  }
}

/// LSM-tree levels manager
/// LSM-tree 层级管理器
pub struct Levels<M> {
  pub li: Vec<Level<M>>,
  pub max_level: u8,
  pub l0_limit: usize,
  pub l1_size: u64,
  pub size_ratio: u64,
}

impl<M: Meta> Levels<M> {
  pub fn new(conf: &[Conf]) -> Self {
    let mut max_level = default::MAX_LEVEL;
    let mut l0_limit = default::L0_LIMIT;
    let mut l1_size = default::L1_SIZE;
    let mut size_ratio = default::SIZE_RATIO;
    for c in conf {
      match *c {
        Conf::MaxLevel(v) => max_level = v,
        Conf::L0Limit(v) => l0_limit = v,
        Conf::L1Size(v) => l1_size = v,
        Conf::SizeRatio(v) => size_ratio = v,
      }
    }
    let li = (0..=max_level).map(Level::new).collect();
    Self {
      li,
      max_level,
      l0_limit,
      l1_size,
      size_ratio,
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

  /// Find first table containing key
  /// 查找第一个包含键的表
  pub fn find_first(&self, key: &[u8]) -> Option<(u8, usize)> {
    for (i, level) in self.li.iter().enumerate() {
      if let Some(idx) = level.find_table(key) {
        return Some((i as u8, idx));
      }
    }
    None
  }

  /// Find all tables containing key
  /// 查找所有包含键的表
  pub fn find_all(&self, key: &[u8]) -> Vec<(u8, usize)> {
    let mut result = Vec::new();
    for (i, level) in self.li.iter().enumerate() {
      if level.is_l0() {
        for (idx, t) in level.iter().enumerate().rev() {
          if t.contains(key) {
            result.push((i as u8, idx));
          }
        }
      } else if let Some(idx) = level.find_table(key) {
        result.push((i as u8, idx));
      }
    }
    result
  }

  /// Iterate all tables overlapping range (O(1) space per level)
  /// 迭代所有与范围重叠的表（每层 O(1) 空间）
  pub fn find_range<'a>(
    &'a self,
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
  ) -> impl Iterator<Item = (u8, usize)> + 'a {
    self
      .li
      .iter()
      .enumerate()
      .flat_map(move |(i, level)| level.overlapping(start, end).map(move |idx| (i as u8, idx)))
  }

  /// Get compaction candidates for level merge
  /// 获取层级合并的压缩候选
  pub fn compaction_candidates(
    &self,
    level: u8,
    target: u8,
    min: &[u8],
    max: &[u8],
  ) -> (Vec<usize>, Vec<usize>) {
    let bound = (Bound::Included(min), Bound::Included(max));
    let src = self.li.get(level as usize).map_or(vec![], |l| {
      if level == 0 {
        (0..l.len()).collect()
      } else {
        l.overlapping(bound.0, bound.1).collect()
      }
    });
    let dst = self
      .li
      .get(target as usize)
      .map_or(vec![], |l| l.overlapping(bound.0, bound.1).collect());
    (src, dst)
  }
}
