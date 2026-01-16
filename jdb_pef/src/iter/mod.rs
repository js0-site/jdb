use crate::pef::Pef;
pub mod rev;

/// Production-ready Iterator: Stateful, Caching, Smart Skipping.
/// 生产级迭代器：有状态、缓存、智能跳过。
pub struct Iter<'a> {
  pef: &'a Pef,

  // Current state
  // 当前状态
  pub chunk_idx: usize,
  pub in_chunk_idx: usize,

  // Cache: Avoid repeated decoding
  // 缓存：避免重复解码
  pub curr_val: Option<u64>,

  // Cache: Current chunk's max value, used for skipping
  // 缓存：当前块的最大值，用于跳过
  pub curr_chunk_max: Option<u64>,

  // Stop condition: Exclusive upper bound
  // 停止条件：独占上界
  pub end_bound: Option<u64>,
}

impl<'a> Iter<'a> {
  pub fn new(pef: &'a Pef) -> Self {
    let mut iter = Self {
      pef,
      chunk_idx: 0,
      in_chunk_idx: 0,
      curr_val: None,
      curr_chunk_max: None,
      end_bound: None,
    };
    // Initialize state
    // 初始化状态
    iter.load_current_chunk_state();
    iter.resolve_val();
    iter
  }

  /// Set exclusive upper bound.
  /// 设置独占上界。
  pub fn with_bound(mut self, bound: u64) -> Self {
    self.end_bound = Some(bound);
    // Re-validate current value
    if let Some(val) = self.curr_val
      && val >= bound
    {
      self.curr_val = None;
    }
    self
  }

  /// Loads metadata (Max value) for the current chunk.
  /// 加载当前块的元数据（最大值）。
  fn load_current_chunk_state(&mut self) {
    if self.chunk_idx < self.pef.chunks.len() {
      self.curr_chunk_max = self.pef.upper.get(self.chunk_idx);
    } else {
      self.curr_chunk_max = None;
    }
  }

  /// Decodes and caches the current value.
  /// 解码并缓存当前值。
  fn resolve_val(&mut self) {
    if self.chunk_idx < self.pef.chunks.len() {
      // SAFETY: chunk_idx < len checked above
      let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx) };
      self.curr_val = chunk.get(self.in_chunk_idx);

      // Check bound
      if let Some(bound) = self.end_bound
        && let Some(val) = self.curr_val
        && val >= bound
      {
        self.curr_val = None;
      }
    } else {
      self.curr_val = None;
    }
  }

  /// Efficiently seek to the first element >= target.
  /// 高效跳转到第一个 >= target 的元素。
  /// Note: This advances the iterator.
  pub fn seek(&mut self, target: u64) -> Option<u64> {
    // Check if target is beyond bound
    if let Some(bound) = self.end_bound
      && target >= bound
    {
      self.curr_val = None;
      return None;
    }

    // 1. Check if current value matches
    if let Some(val) = self.curr_val
      && val >= target
    {
      return Some(val);
    }

    // 2. Check if we can skip the current chunk?
    if let Some(max) = self.curr_chunk_max
      && target > max
    {
      // Forward to next chunk candidates
      match self.pef.upper.next_ge_from(self.chunk_idx + 1, target) {
        Some((idx, _max_val)) => {
          self.chunk_idx = idx;
          self.in_chunk_idx = 0;
          self.load_current_chunk_state();
          self.resolve_val();
        }
        None => {
          self.chunk_idx = self.pef.chunks.len();
          self.curr_val = None;
          return None;
        }
      }
    }

    // 3. Intra-chunk search
    if self.chunk_idx < self.pef.chunks.len() {
      let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx) };
      match chunk.next_ge_from(self.in_chunk_idx, target) {
        Some((idx, val)) => {
          // Check bound
          if let Some(bound) = self.end_bound
            && val >= bound
          {
            self.curr_val = None;
            return None;
          }
          self.in_chunk_idx = idx;
          self.curr_val = Some(val);
          Some(val)
        }
        None => {
          // Fallback to next chunk if not found in current
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

      if self.chunk_idx < self.pef.chunks.len() {
        let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx) };
        if self.in_chunk_idx < chunk.n {
          self.resolve_val();
        } else {
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
