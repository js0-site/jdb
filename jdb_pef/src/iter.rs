use crate::pef::Pef;

/// Production-ready Iterator: Stateful, Caching, Smart Skipping.
/// 生产级迭代器：有状态、缓存、智能跳过。
/// Formerly `Cursor`.
/// 前身为 `Cursor`。
pub struct Iter<'a> {
  pef: &'a Pef,

  // Current state
  // 当前状态
  chunk_idx: usize,
  in_chunk_idx: usize,

  // Cache: Avoid repeated decoding
  // 缓存：避免重复解码
  // If Some(v), it means we are currently pointing to this value
  curr_val: Option<u64>,

  // Cache: Current chunk's max value, used for skipping
  // 缓存：当前块的最大值，用于跳过
  curr_chunk_max: Option<u64>,
}

impl<'a> Iter<'a> {
  pub fn new(pef: &'a Pef) -> Self {
    let mut iter = Self {
      pef,
      chunk_idx: 0,
      in_chunk_idx: 0,
      curr_val: None,
      curr_chunk_max: None,
    };
    // Initialize state
    // 初始化状态
    iter.load_current_chunk_state();
    iter.resolve_val();
    iter
  }

  /// Loads metadata (Max value) for the current chunk.
  /// 加载当前块的元数据（最大值）。
  fn load_current_chunk_state(&mut self) {
    if self.chunk_idx < self.pef.chunks.len() {
      // Upper index stores max values of chunks
      // 上层索引存储块的最大值
      self.curr_chunk_max = self.pef.upper.get(self.chunk_idx);
    } else {
      self.curr_chunk_max = None;
    }
  }

  /// Decodes and caches the current value.
  /// 解码并缓存当前值。
  fn resolve_val(&mut self) {
    if self.chunk_idx < self.pef.chunks.len() {
      // Optimization: use get_unchecked since chunk_idx < len
      // 优化：因为 chunk_idx < len，使用/可使用 get_unchecked
      // SAFETY: In bounds by logic
      let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx) };
      self.curr_val = chunk.get(self.in_chunk_idx);
    } else {
      self.curr_val = None;
    }
  }

  /// Efficiently seek to the first element >= target.
  /// 高效跳转到第一个 >= target 的元素。
  /// Note: This advances the iterator.
  pub fn seek(&mut self, target: u64) -> Option<u64> {
    // 1. Check if current value matches
    // 1. 检查当前值是否匹配
    if let Some(val) = self.curr_val
      && val >= target
    {
      return Some(val);
    }

    // 2. Check if we can skip the current chunk?
    // 2. 检查是否可以跳过当前块？
    // If target > curr_chunk_max, we must move to next chunks
    if let Some(max) = self.curr_chunk_max
      && target > max
    {
      // Forward to next chunk candidates
      // 前进到下一个候选块
      // Use upper index to find which chunk *might* contain target
      // 使用上层索引查找哪个块*可能*包含 target
      match self.pef.upper.next_ge_from(self.chunk_idx + 1, target) {
        Some((idx, _max_val)) => {
          self.chunk_idx = idx;
          self.in_chunk_idx = 0;
          self.load_current_chunk_state();
          self.resolve_val();
        }
        None => {
          // End of list
          // 列表结束
          self.chunk_idx = self.pef.chunks.len();
          self.curr_val = None;
          return None;
        }
      }
    }

    // 3. Intra-chunk search
    // 3. 块内搜索
    if self.chunk_idx < self.pef.chunks.len() {
      let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx) };
      // Use next_ge_from with hinting
      match chunk.next_ge_from(self.in_chunk_idx, target) {
        Some((idx, val)) => {
          self.in_chunk_idx = idx;
          self.curr_val = Some(val);
          Some(val)
        }
        None => {
          // Fallback to next chunk if not found in current (rare edge case logic)
          // 如果在当前块中未找到，则回退到下一个块（罕见的边缘情况逻辑）
          self.chunk_idx += 1;
          self.in_chunk_idx = 0;
          self.load_current_chunk_state();
          self.resolve_val();
          self.seek(target)
        }
      }
    } else {
      None
    }
  }

  /// Get current value without moving.
  /// 获取当前值而不移动。
  pub fn value(&self) -> Option<u64> {
    self.curr_val
  }
}

impl<'a> Iterator for Iter<'a> {
  type Item = u64;

  fn next(&mut self) -> Option<Self::Item> {
    let ret = self.curr_val;

    if ret.is_some() {
      self.in_chunk_idx += 1;

      // Check if we can stay in current chunk
      // 检查是否可以停留在当前块
      if self.chunk_idx < self.pef.chunks.len() {
        let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx) };
        if self.in_chunk_idx < chunk.n {
          self.curr_val = chunk.get(self.in_chunk_idx);
        } else {
          // Move to next chunk
          // 移动到下一个块
          self.chunk_idx += 1;
          self.in_chunk_idx = 0;
          self.load_current_chunk_state();
          self.resolve_val();
        }
      } else {
        self.curr_val = None;
      }
    }
    ret
  }
}
