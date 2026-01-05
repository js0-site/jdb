//! PGM-Index main implementation
//! PGM 索引主实现

#![allow(clippy::cast_precision_loss)]

use std::mem::size_of;

use crate::{
  Key, PGMStats, Segment,
  error::{PGMError, Result},
  pgm::{
    build::{build_lut, build_segments},
    consts::MIN_EPSILON,
    search::predict,
  },
};

/// PGM-Index structure
/// PGM 索引结构
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
#[derive(Clone, Debug)]
pub struct PGMIndex<K: Key> {
  // Fields are private. Order matters for struct packing/padding,
  // but Vecs are usually 24 bytes (ptr+cap+len), so order is less critical here.
  epsilon: usize,
  data: Vec<K>,
  segments: Vec<Segment<K>>,
  lut: Vec<usize>,
  scale: f64,
  min_key: f64,
}

impl<K: Key> PGMIndex<K> {
  /// Load PGM-Index from sorted data (O(N) build time)
  /// 从已排序数据加载 PGM 索引
  ///
  /// # Arguments
  /// * `check_sorted`: If false, skips O(N) sort check (Unsafe if data is not sorted!)
  ///
  /// # Errors
  /// Returns `PGMError::InvalidEpsilon` if epsilon < `MIN_EPSILON`
  /// Returns `PGMError::EmptyData` if data is empty
  /// Returns `PGMError::NotSorted` if `check_sorted` is true and data is not sorted
  pub fn load(data: Vec<K>, epsilon: usize, check_sorted: bool) -> Result<Self> {
    if epsilon < MIN_EPSILON {
      return Err(PGMError::InvalidEpsilon {
        provided: epsilon,
        min: MIN_EPSILON,
      });
    }
    if data.is_empty() {
      return Err(PGMError::EmptyData);
    }

    // OPT: Allow skipping expensive check
    if check_sorted && !is_sorted_fast(&data) {
      return Err(PGMError::NotSorted);
    }

    let segments = build_segments(&data, epsilon);
    let (lut, scale, min_key) = build_lut(&data, &segments);

    Ok(Self {
      epsilon,
      data,
      segments,
      lut,
      scale,
      min_key,
    })
  }

  /// Get statistics
  /// 获取统计信息
  #[inline]
  #[must_use]
  pub fn stats(&self) -> PGMStats {
    PGMStats {
      segments: self.segments.len(),
      avg_segment_size: self.data.len() as f64 / self.segments.len().max(1) as f64,
      memory_bytes: self.mem_usage(),
    }
  }

  #[inline]
  #[must_use]
  pub fn segment_count(&self) -> usize {
    self.segments.len()
  }

  #[inline]
  #[must_use]
  pub fn avg_segment_size(&self) -> f64 {
    self.data.len() as f64 / self.segments.len().max(1) as f64
  }

  #[inline]
  #[must_use]
  pub fn mem_usage(&self) -> usize {
    self.data.len() * size_of::<K>()
      + self.segments.len() * size_of::<Segment<K>>()
      + self.lut.len() * size_of::<usize>()
  }

  #[inline]
  #[must_use]
  pub fn memory_usage(&self) -> usize {
    self.mem_usage()
  }

  /// Get reference to underlying data
  /// 获取底层数据引用
  #[inline]
  #[must_use]
  pub fn data(&self) -> &[K] {
    &self.data
  }

  #[inline]
  #[must_use]
  pub fn epsilon(&self) -> usize {
    self.epsilon
  }

  /// Get position of key (None if absent)
  /// 获取键的位置（不存在则返回 None）
  #[inline]
  #[must_use]
  pub fn get(&self, key: K) -> Option<usize> {
    // OPT: manual inline of find_seg to avoid call overhead and expose fast path
    // 1. Find the approximate segment via LUT
    let seg = if self.segments.len() <= 1 {
      unsafe { self.segments.get_unchecked(0) }
    } else {
      let y = key.as_f64();
      let idx_candidate = (y - self.min_key) * self.scale;
      let lut_max = (self.lut.len() - 1) as isize;

      let idx_i = idx_candidate as isize;
      let bin = if idx_i < 0 {
        0
      } else if idx_i > lut_max {
        lut_max as usize
      } else {
        idx_i as usize
      };

      // SAFETY: bin is clamped
      let mut idx = unsafe { *self.lut.get_unchecked(bin) };

      // OPT: Optimistic check - LUT usually points to the correct segment or close to it
      // Fast path: key is within the segment pointed by LUT
      let mut seg = unsafe { self.segments.get_unchecked(idx) };

      // Handle the rare case where we need to scan forward
      while idx + 1 < self.segments.len() {
        if key <= seg.max_key {
          break;
        }
        idx += 1;
        seg = unsafe { self.segments.get_unchecked(idx) };
      }

      // Handle the rare case where we need to scan backward
      // (This works but looking at logic, lut always points to segment covering min?
      //  Original find_seg had a backward scan. Keeping it for correctness.)
      while idx > 0 {
        if key >= seg.min_key {
          break;
        }
        idx -= 1;
        seg = unsafe { self.segments.get_unchecked(idx) };
      }
      seg
    };

    // 2. Check if key is within the segment's min/max range (Bloom filter effect)
    if key < seg.min_key || key > seg.max_key {
      return None;
    }

    let predicted = predict(seg, key.as_f64());

    // OPT: Constrain search range within segment bounds
    // 优化：将搜索范围限制在段边界内
    // The PGM guarantee: position is within [predicted - epsilon, predicted + epsilon]
    // PGM 保证：位置在 [predicted - epsilon, predicted + epsilon] 范围内
    let start = predicted.saturating_sub(self.epsilon).max(seg.start_idx);
    let end = (predicted.saturating_add(self.epsilon + 1)).min(seg.end_idx);

    // SAFETY: start and end are derived from valid segment indices which are bound by data.len().
    // Using get_unchecked for performance in hot path.
    // 优化：在热路径中使用 get_unchecked (前提是 build 逻辑保证了索引有效性)
    unsafe {
      let slice = self.data.get_unchecked(start..end);
      // Note: binary_search returns any match for duplicates.
      // 注意：对于重复项，binary_search 返回任意匹配项。
      if let Ok(pos) = slice.binary_search(&key) {
        return Some(start + pos);
      }
    }
    None
  }

  /// Batch lookup returning an iterator
  /// 批量查找（返回迭代器，O(1) 空间复杂度）
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

  /// Get predicted position for a key (for accuracy benchmarking)
  /// 获取键的预测位置（用于精度基准测试）
  #[inline]
  #[must_use]
  pub fn predict_pos(&self, key: K) -> usize {
    let seg = self.find_seg(key);
    predict(seg, key.as_f64())
  }

  /// Find segment for a key
  /// 查找键所属的段
  #[inline]
  fn find_seg(&self, key: K) -> &Segment<K> {
    if self.segments.len() <= 1 {
      unsafe { self.segments.get_unchecked(0) }
    } else {
      let y = key.as_f64();
      let idx_candidate = (y - self.min_key) * self.scale;
      let lut_max = (self.lut.len() - 1) as isize;

      let idx_i = idx_candidate as isize;
      let bin = if idx_i < 0 {
        0
      } else if idx_i > lut_max {
        lut_max as usize
      } else {
        idx_i as usize
      };

      let mut idx = unsafe { *self.lut.get_unchecked(bin) };
      let mut seg = unsafe { self.segments.get_unchecked(idx) };

      while idx + 1 < self.segments.len() {
        if key <= seg.max_key {
          break;
        }
        idx += 1;
        seg = unsafe { self.segments.get_unchecked(idx) };
      }

      while idx > 0 {
        if key >= seg.min_key {
          break;
        }
        idx -= 1;
        seg = unsafe { self.segments.get_unchecked(idx) };
      }
      seg
    }
  }
}

// Optimized sorted check avoiding slice iterator overhead
#[inline]
fn is_sorted_fast<K: Ord>(data: &[K]) -> bool {
  // OPT: Use `windows` which compiles to efficient vectorized code (SIMD)
  // and eliminates bounds checks in the loop. Safer and equally fast.
  // 优化：使用 `windows`，编译器会生成高效的向量化代码并消除循环内的边界检查。
  data.windows(2).all(|w| w[0] <= w[1])
}
