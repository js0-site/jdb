use crate::pef::Pef;

/// Reverse Iterator for Pef.
/// Pef 的反向迭代器。
pub struct RevIter<'a> {
  pef: &'a Pef,

  // Current cursor position
  // 当前游标位置
  chunk_idx: isize, // Use isize to handle -1 easily, or usize and be careful
  in_chunk_idx: isize,

  // Cache
  curr_val: Option<u64>,

  // Stop condition: strict lower bound (inclusive)
  // 停止条件：严格下界（包含）
  // Iterate while val >= min_bound
  min_bound: u64,
}

impl<'a> RevIter<'a> {
  pub fn new(pef: &'a Pef, start_idx_opt: Option<(usize, usize)>, min_bound: u64) -> Self {
    let (c, i) = match start_idx_opt {
      Some((c, i)) => (c as isize, i as isize),
      None => {
        // Point to last element if any
        if pef.chunks.is_empty() {
          (-1, -1)
        } else {
          let last_c = pef.chunks.len() - 1;
          let last_n = pef.chunks[last_c].n;
          if last_n == 0 {
            // Scan back in case last chunks are empty (unlikely in Pef construction but possible)
            // For now assume constructed Pef has data or check logic
            // logic simplified
            (last_c as isize, -1) // Point before start? No wait.
          // We want to point TO the last element.
          // last element is at (last_c, last_n - 1)
          } else {
            (last_c as isize, (last_n - 1) as isize)
          }
        }
      }
    };

    let mut iter = Self {
      pef,
      chunk_idx: c,
      in_chunk_idx: i,
      curr_val: None,
      min_bound,
    };

    // Resolve initial value
    iter.resolve_val();

    // Check bound immediately? (if initial value < min_bound, it's invalid)
    if let Some(v) = iter.curr_val
      && v < min_bound
    {
      iter.curr_val = None;
    }

    iter
  }

  pub fn empty(pef: &'a Pef) -> Self {
    Self {
      pef,
      chunk_idx: -1,
      in_chunk_idx: -1,
      curr_val: None,
      min_bound: 0,
    }
  }

  fn resolve_val(&mut self) {
    if self.chunk_idx < 0 {
      self.curr_val = None;
      return;
    }

    let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx as usize) };

    if self.in_chunk_idx < 0 || self.in_chunk_idx >= chunk.n as isize {
      // Out of bounds (e.g. init or moved past)
      self.curr_val = None;
    } else {
      self.curr_val = chunk.get(self.in_chunk_idx as usize);
    }
  }

  fn move_prev(&mut self) {
    if self.chunk_idx < 0 {
      return;
    }

    self.in_chunk_idx -= 1;

    if self.in_chunk_idx < 0 {
      // Move to previous chunk
      self.chunk_idx -= 1;
      if self.chunk_idx >= 0 {
        let chunk = unsafe { self.pef.chunks.get_unchecked(self.chunk_idx as usize) };
        self.in_chunk_idx = (chunk.n as isize) - 1;
      }
    }
  }
}

impl<'a> Iterator for RevIter<'a> {
  type Item = u64;

  fn next(&mut self) -> Option<Self::Item> {
    let ret = self.curr_val;

    if ret.is_some() {
      // Move to previous
      self.move_prev();
      self.resolve_val();

      // Check bound on new value (for next call)?
      // No, we cache `curr_val` for *next* call.
      // If `curr_val` < min_bound, clear it.
      if let Some(v) = self.curr_val
        && v < self.min_bound
      {
        self.curr_val = None;
      }
    }

    ret
  }
}
