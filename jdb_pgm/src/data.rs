//! Pgm-Index with data ownership
//! 持有数据的 Pgm 索引

use std::{mem::size_of, ops::Deref};

use crate::{Key, Pgm, Result};

/// Linear scan threshold
/// 线性扫描阈值
const LINEAR_THRESHOLD: usize = 32;

/// Pgm-Index with data ownership
/// 持有数据的 Pgm 索引
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
#[derive(Clone, Debug)]
pub struct PgmData<K: Key> {
  pub pgm: Pgm<K>,
  pub data: Vec<K>,
}

impl<K: Key> Deref for PgmData<K> {
  type Target = Pgm<K>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.pgm
  }
}

impl<K: Key> PgmData<K> {
  /// Load Pgm-Index from sorted data
  /// 从已排序数据加载 Pgm 索引
  pub fn load(data: Vec<K>, epsilon: usize, check_sorted: bool) -> Result<Self> {
    let pgm = Pgm::new(&data, epsilon, check_sorted)?;
    Ok(Self { pgm, data })
  }

  /// Get reference to underlying data
  /// 获取底层数据引用
  #[inline]
  #[must_use]
  pub fn data(&self) -> &[K] {
    &self.data
  }

  /// Get position of key (None if absent)
  /// 获取键的位置（不存在则返回 None）
  #[inline]
  #[must_use]
  pub fn get(&self, key: K) -> Option<usize> {
    let (lo, hi) = self.pgm.predict_range(key);
    if lo >= self.data.len() {
      return None;
    }
    let hi = hi.min(self.data.len());
    let len = hi - lo;
    
    if len <= LINEAR_THRESHOLD {
      // Linear scan for small ranges
      // 小范围线性扫描
      for i in lo..hi {
        let v = unsafe { *self.data.get_unchecked(i) };
        if v == key {
          return Some(i);
        }
        if v > key {
          return None;
        }
      }
      None
    } else {
      // Binary search for large ranges
      // 大范围二分查找
      unsafe { self.data.get_unchecked(lo..hi) }
        .binary_search(&key)
        .ok()
        .map(|p| lo + p)
    }
  }

  /// Batch lookup returning an iterator
  /// 批量查找（返回迭代器）
  #[inline]
  pub fn get_many<'a, I>(&'a self, keys: I) -> impl Iterator<Item = Option<usize>> + 'a
  where
    I: IntoIterator<Item = K> + 'a,
    <I as IntoIterator>::IntoIter: 'a,
  {
    keys.into_iter().map(move |k| self.get(k))
  }

  /// Count hits in batch
  /// 批量命中计数
  #[inline]
  pub fn count_hits<I>(&self, keys: I) -> usize
  where
    I: IntoIterator<Item = K>,
  {
    keys.into_iter().filter(|&k| self.get(k).is_some()).count()
  }

  /// Memory usage (including data)
  /// 内存占用（含数据）
  #[inline]
  #[must_use]
  pub fn memory_usage(&self) -> usize {
    self.data.len() * size_of::<K>() + self.pgm.mem_usage()
  }

  /// Get predicted position for a key (for benchmarking)
  /// 获取键的预测位置（用于基准测试）
  #[inline]
  #[must_use]
  pub fn predict_pos(&self, key: K) -> usize {
    self.pgm.predict(key)
  }

  /// Get statistics
  /// 获取统计信息
  #[inline]
  #[must_use]
  pub fn stats(&self) -> crate::PgmStats {
    crate::PgmStats {
      segments: self.pgm.segment_count(),
      avg_segment_size: self.pgm.avg_segment_size(),
      memory_bytes: self.memory_usage(),
    }
  }
}
