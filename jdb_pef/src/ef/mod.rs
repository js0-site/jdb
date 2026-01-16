use crate::bits::Bv;

pub mod sel;
pub use sel::Sel;

/// Core Elias-Fano compression structure.
/// 核心 Elias-Fano 压缩结构。
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct Ef {
  pub high_bits: Bv,
  pub low_bits: Bv,
  pub select_index: Sel,

  pub universe: u64,
  pub n: usize,
  pub low_len: usize,
  pub min_val: u64,
}

impl Ef {
  /// Construct a new Elias-Fano index from sorted values with default configuration.
  /// 使用默认配置从排序值构建新的 Elias-Fano 索引。
  pub fn new(values: &[u64]) -> Self {
    Self::new_with_conf(values, crate::conf::Conf::default())
  }

  /// Construct a new Elias-Fano index with custom configuration.
  /// 使用自定义配置构建新的 Elias-Fano 索引。
  pub fn new_with_conf(values: &[u64], conf: crate::conf::Conf) -> Self {
    if values.is_empty() {
      return Self::empty();
    }

    let n = values.len();
    let min_val = values[0];

    let max_val = values[n - 1];
    let range = max_val - min_val; // Relative universe / 相对全集

    let u = std::cmp::max(range + 1, n as u64);

    // l = floor(log2(U / N))
    let low_len = if u > n as u64 {
      (u as f64 / n as f64).log2().floor() as usize
    } else {
      0
    };

    let mut high_bits = Bv::new();
    let mut low_bits = Bv::new();

    let mut current_high = 0;

    for &v in values {
      let val = v - min_val;
      let h = (val >> low_len) as usize;
      let l_part = val & ((1 << low_len) - 1);

      // Unary coding for high bits: write (h - current_high) zeros, then one 1
      // 高位的的一元编码：写入 (h - current_high) 个 0，然后写入一个 1
      while current_high < h {
        high_bits.push(false);
        current_high += 1;
      }
      high_bits.push(true);

      // Fixed width coding for low bits
      // 低位的定宽编码
      low_bits.push_int(l_part, low_len);
    }

    high_bits.shrink_to_fit();
    low_bits.shrink_to_fit();

    let select_index = Sel::new(&high_bits, n, conf);

    Self {
      high_bits,
      low_bits,
      select_index,
      universe: u,
      n,
      low_len,
      min_val,
    }
  }

  /// Create an empty index.
  /// 创建空索引。
  pub fn empty() -> Self {
    Self {
      high_bits: Bv::new(),
      low_bits: Bv::new(),
      select_index: Sel::default(),
      universe: 0,
      n: 0,
      low_len: 0,
      min_val: 0,
    }
  }
}

impl Default for Ef {
  fn default() -> Self {
    Self::empty()
  }
}

impl Ef {
  /// Select(rank): Find position of the rank-th 1 in high_bits.
  /// Select(rank): 在 high_bits 中查找第 rank 个 1 的位置。
  #[inline]
  fn select(&self, rank: usize) -> usize {
    let bit_pos = self.select_index.get_search_start(rank);
    let current_rank = self.select_index.get_rank_start(rank);

    // Word-level scan
    let mut word_idx = bit_pos / 64;
    let bit_offset = bit_pos % 64;

    let data = &self.high_bits.data;

    // Handle first word potentially partially
    if word_idx < data.len() {
      let mut w = unsafe { *data.get_unchecked(word_idx) };

      // Mask out bits before bit_offset
      // We want to count ones starting from bit_offset
      // w = w & !((1<<bit_offset) - 1); // clear lower bits?
      // "bit_pos" is the search START. It might be the exact position or before it.
      // If it's a 0-bit, we skip.
      // But select_index gives "position of every N-th 1".
      // So bit_pos is exactly the position of (N*i)-th 1.
      // So current_rank is exactly the rank at bit_pos.

      // If bit_pos is exactly rank, we return it.
      // Wait, select(rank) = position.
      // get_search_start -> bit_pos of (rank/sampling * sampling)-th 1.
      // So at bit_pos, the rank is `current_rank`.
      // But Bv::push might push 0s.
      // The 1 is at bit_pos.

      if current_rank == rank {
        return bit_pos;
      }

      // We need to find (rank - current_rank) more 1s.
      let needed = rank - current_rank;

      // Mask lower bits to ignore ones we've already counted (sampled)
      // But bit_pos IS the sampled 1. We start SEARCHING from bit_pos + 1.
      // So we want to ignore bits <= bit_pos.

      // Let's refine the loop.
      // We start at word_idx.
      // We should mask bits < (bit_pos + 1) % 64 ?
      // Actually, we can just mask bits <= bit_offset.
      // Because bit_pos is a 1. rank(bit_pos) = current_rank.
      // We want (rank). So we need (rank - current_rank) more 1s AFTER bit_pos.

      // w >>= (bit_offset + 1); // shift away
      // effective bit index becomes: word_idx * 64 + bit_offset + 1 + found_idx

      // L2_RATE is 32.
      // In dense regions (common in EF high bits), 32 ones span about 64 bits.
      // So we likely finish in 1st or 2nd word.

      // 1. Process first word
      if bit_offset == 63 {
        w = 0;
      } else {
        w >>= bit_offset + 1;
      }

      let ones = w.count_ones() as usize;
      if needed <= ones {
        let found_idx = crate::utils::select64(w, needed - 1);
        return word_idx * 64 + bit_offset + 1 + found_idx;
      }

      let mut needed = needed - ones;
      word_idx += 1;

      // 2. Process subsequent words
      // Unroll slightly?
      // With L2=32, this loop runs very few times.
      while word_idx < data.len() {
        let w = unsafe { *data.get_unchecked(word_idx) };
        let ones = w.count_ones() as usize;
        if needed <= ones {
          let found_idx = crate::utils::select64(w, needed - 1);
          return word_idx * 64 + found_idx;
        }
        needed -= ones;
        word_idx += 1;
      }
    }

    // Should not reach here if rank is valid
    self.n
  }

  /// Read the index-th value.
  /// 读取第 index 个值。
  pub fn get(&self, index: usize) -> Option<u64> {
    if index >= self.n {
      return None;
    }

    // 1. High part: count zeros before the index-th one (unary decoding)
    // 1. 高位部分：统计第 index 个 1 之前的 0 的数量（一元解码）
    let pos = self.select(index);
    let high_val = (pos - index) as u64;

    // 2. Low part
    // 2. 低位部分
    // SAFETY: index < self.n, so index * low_len is valid.
    // 安全性：index < self.n，故 index * low_len 有效。
    let low_val = unsafe {
      self
        .low_bits
        .get_int_unchecked(index * self.low_len, self.low_len)
    };

    // 3. Combine
    // 3. 组合
    let val = (high_val << self.low_len) | low_val;
    Some(val + self.min_val)
  }

  /// Find first value >= target starting from start_idx.
  /// 从 start_idx 开始查找第一个 >= target 的值。
  /// Returns (index, value).
  /// 返回 (index, value)。
  pub fn next_ge_from(&self, start_idx: usize, target: u64) -> Option<(usize, u64)> {
    if start_idx >= self.n {
      return None;
    }

    if target > self.min_val.saturating_add(self.universe) {
      return None;
    }

    // Binary search for robustness and speed on larger gaps
    // 二分查找以提高鲁棒性及大间距时的速度
    let mut low = start_idx;
    let mut high = self.n;

    while low < high {
      let mid = low + (high - low) / 2;
      // SAFETY: mid is always < self.n because high start at n, low starts at <n and high decreases or low increases
      // We use unwrap_unchecked to avoid check as we know mid < n
      let val = unsafe { self.get(mid).unwrap_unchecked() };
      if val < target {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    if low < self.n {
      // Optimization: we know low is valid
      unsafe { Some((low, self.get(low).unwrap_unchecked())) }
    } else {
      None
    }
  }

  /// Find first value >= target.
  /// 查找第一个 >= target 的值。
  pub fn next_ge(&self, target: u64) -> Option<(usize, u64)> {
    self.next_ge_from(0, target)
  }

  /// Returns memory size in bytes.
  /// 返回内存大小（字节）。
  pub fn size_in_bytes(&self) -> usize {
    let sel_size =
      self.select_index.l1_positions.len() * 8 + self.select_index.l2_offsets.len() * 2;

    self.high_bits.size_in_bytes() + self.low_bits.size_in_bytes() + sel_size
  }
}
