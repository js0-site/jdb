use crate::{
  ef::Ef,
  iter::{Iter, rev::RevIter},
};

/// Partitioned Elias-Fano Index.
/// 分区 Elias-Fano 索引。
/// Contains two levels: Upper (Headers) and Lower (Chunks).
/// 包含两层：上层（头信息）和下层（数据块）。
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct Pef {
  pub upper: Ef,
  pub chunks: Vec<Ef>,
  pub block_size: usize,
  pub num_elements: usize,
}

impl Pef {
  /// Create a new Partitioned Elias-Fano index.
  /// 创建一个新的分区 Elias-Fano 索引。
  ///
  /// `data`: Sorted sequence of integers.
  /// Construct a new Pef with default configuration.
  pub fn new(data: &[u64]) -> Self {
    Self::new_with_conf(data, crate::conf::Conf::default())
  }

  /// Construct with custom config.
  pub fn new_with_conf(data: &[u64], conf: crate::conf::Conf) -> Self {
    let block_size = conf.block_size;
    assert!(block_size > 0, "block_size must be > 0");
    if data.is_empty() {
      return Self {
        upper: Ef::empty(),
        chunks: vec![],
        block_size,
        num_elements: 0,
      };
    }

    let mut chunks = Vec::with_capacity(data.len().div_ceil(block_size));
    let mut upper_bounds = Vec::with_capacity(chunks.capacity());

    for chunk_slice in data.chunks(block_size) {
      chunks.push(Ef::new_with_conf(chunk_slice, conf));
      // Record the max value of each chunk
      // 记录每个块的最大值
      // SAFETY: chunks are non-empty because data is non-empty and chunk_slice is from data
      unsafe {
        upper_bounds.push(*chunk_slice.last().unwrap_unchecked());
      }
    }

    // Build upper level index
    // 构建上层索引
    let upper = Ef::new_with_conf(&upper_bounds, conf);

    chunks.shrink_to_fit();

    Self {
      upper,
      chunks,
      block_size,
      num_elements: data.len(),
    }
  }

  /// Random access: get the index-th element.
  /// 随机访问：获取第 index 个元素。
  pub fn get(&self, index: usize) -> Option<u64> {
    if index >= self.num_elements {
      return None;
    }

    // Optimization: use unchecked division/mod if we trust block_size > 0
    // 优化：如果相信 block_size > 0，可使用 unchecked 除法/取模
    let chunk_idx = index / self.block_size;
    let offset = index % self.block_size;

    // SAFETY: chunk_idx is generally safe but let's keep safe get for robustness.
    // We can use get_unchecked safely because:
    // index < num_elements
    // num_elements = sum(chunk_len)
    // so chunk_idx < chunks.len()
    unsafe { self.chunks.get_unchecked(chunk_idx).get(offset) }
  }

  /// Find first element >= target.
  /// 查找第一个 >= target 的元素。
  pub fn next_ge(&self, target: u64) -> Option<u64> {
    // 1. Search in Upper layer
    // 1. 在上层索引中搜索
    // We look for a chunk whose Max >= target
    // 我们寻找一个 Max >= target 的块
    match self.upper.next_ge(target) {
      Some((chunk_idx, _max_val)) => {
        // 2. Search in specific chunk
        // 2. 在特定块中搜索
        if chunk_idx >= self.chunks.len() {
          return None;
        }
        // SAFETY: indexing chunks is safe as checked above?
        // Wait, upper index stores MAX values.
        // If chunk_idx from upper is valid, chunks[chunk_idx] exists.
        unsafe {
          self
            .chunks
            .get_unchecked(chunk_idx)
            .next_ge(target)
            .map(|(_idx, val)| val)
        }
      }
      None => None,
    }
  }

  /// Create an iterator for efficient sequential access and skipping.
  /// 创建一个用于高效顺序访问和跳过的迭代器。
  pub fn iter(&self) -> Iter<'_> {
    Iter::new(self)
  }

  /// Create a reverse iterator.
  /// 创建一个反向迭代器。
  pub fn rev_iter(&self) -> RevIter<'_> {
    RevIter::new(self, None, 0)
  }

  /// Create a range iterator [start, end).
  /// 创建一个范围迭代器 [start, end)。
  pub fn range(&self, range: std::ops::Range<u64>) -> Iter<'_> {
    let mut it = Iter::new(self).with_bound(range.end);
    if range.start > 0 {
      it.seek(range.start);
    }
    it
  }

  /// Create a reverse range iterator elements in [start, end) iterated backwards.
  /// 创建一个反向范围迭代器，[start, end) 内的元素反向迭代。
  /// Stops when element < start.
  pub fn rev_range(&self, range: std::ops::Range<u64>) -> RevIter<'_> {
    let start_pos = if range.end > 0 {
      self.find_last_lt(range.end)
    } else {
      None
    };

    match start_pos {
      Some(p) => RevIter::new(self, Some(p), range.start),
      None => RevIter::empty(self),
    }
  }

  /// Helper: Find the index of the largest element < target.
  /// 辅助函数：查找小于 target 的最大元素的索引。
  fn find_last_lt(&self, target: u64) -> Option<(usize, usize)> {
    if self.chunks.is_empty() {
      return None;
    }

    // 1. Search in Upper layer to find the first chunk that MIGHT contain a value >= target.
    // 1. 在上层中搜索，找到第一个可能包含 >= target 值的块。
    // The chunks before this one definitively have all values < target.
    match self.upper.next_ge(target) {
      Some((chunk_idx, _)) => {
        if chunk_idx >= self.chunks.len() {
          // Should capture this in None case usually but just in case
          return self.last_pos();
        }

        // 2. Search in specific chunk
        // 2. 在特定块中搜索
        // SAFETY: chunk_idx validated
        let chunk = unsafe { self.chunks.get_unchecked(chunk_idx) };

        // Find first element >= target in this chunk
        match chunk.next_ge(target) {
          Some((in_idx, _val)) => {
            if in_idx > 0 {
              // Standard case: Previous element in same chunk
              Some((chunk_idx, in_idx - 1))
            } else {
              // First element in chunk is >= target.
              // So the desired element is the last element of the previous chunk.
              self.prev_pos(chunk_idx)
            }
          }
          None => {
            // This implies all elements in this chunk are < target.
            // This contradicts `upper.next_ge(target)` which says max >= target.
            // Only possible if chunk is empty (should not happen) or upper is loose (not exact).
            // Assuming upper stores exact max, this is unreachable unless bug.
            // Fallback: This chunk is all < target, so last element is valid.
            Some((chunk_idx, chunk.n.saturating_sub(1)))
          }
        }
      }
      None => {
        // No chunk has max >= target. So ALL elements are < target.
        // Start from the very last element.
        self.last_pos()
      }
    }
  }

  /// Helper: Index of the very last element.
  fn last_pos(&self) -> Option<(usize, usize)> {
    if self.chunks.is_empty() {
      return None;
    }
    let c = self.chunks.len() - 1;
    let n = self.chunks[c].n;
    if n > 0 { Some((c, n - 1)) } else { None } // Handle empty last chunk case
  }

  /// Helper: Index of the last element of the previous chunk.
  fn prev_pos(&self, chunk_idx: usize) -> Option<(usize, usize)> {
    if chunk_idx == 0 {
      return None;
    }
    let c = chunk_idx - 1;
    let n = self.chunks[c].n;
    if n > 0 {
      Some((c, n - 1))
    } else {
      self.prev_pos(c)
    } // Recurse if empty chunk
  }

  /// Memory usage in bytes.
  /// 内存使用量（字节）。
  pub fn memory_usage(&self) -> usize {
    self.upper.size_in_bytes() + self.chunks.iter().map(|c| c.size_in_bytes()).sum::<usize>()
  }
}

// Implement IntoIterator for &Pef
// 为 &Pef 实现 IntoIterator
impl<'a> IntoIterator for &'a Pef {
  type Item = u64;
  type IntoIter = Iter<'a>;

  fn into_iter(self) -> Self::IntoIter {
    self.iter()
  }
}
