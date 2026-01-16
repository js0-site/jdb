use crate::bits::Bv;

/// Used to accelerate Select operations.
/// 用于加速 Select 操作。
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct Sel {
  // Stores the position of every N-th 1
  // 存储每 N 个 1 的位置
  pub(crate) positions: Vec<usize>,
  pub(crate) sampling_rate: usize,
}

impl Sel {
    /// Create a new SelectIndex.
    /// 创建一个新的 SelectIndex。
    pub fn new(bv: &Bv, n_ones: usize, sampling_rate: usize) -> Self {
        if n_ones == 0 || sampling_rate == 0 {
            return Self {
                positions: vec![],
                sampling_rate,
            };
        }

        let mut positions = Vec::with_capacity(n_ones / sampling_rate + 1);
        let mut ones_count = 0;

        // Iterate over words directly for performance
        // 直接遍历字以提高性能
        let mut total_bits_processed = 0;
        
        for &word in &bv.data {
            let mut w = word;
            // Iterate bits in word
            // 遍历字中的位
            // Simple loop 0..64 is better for branch prediction than `while w > 0` usually, 
            // but we need accurate position.
            // 简单的 0..64 循环通常比 `while w > 0` 更有利于分支预测，但我们需要精确位置。
            for _ in 0..64 {
                if total_bits_processed >= bv.len {
                     break; 
                }
                
                if (w & 1) == 1 {
                    if ones_count % sampling_rate == 0 {
                        positions.push(total_bits_processed);
                    }
                    ones_count += 1;
                }
                w >>= 1;
                total_bits_processed += 1;
            }
        }

        Self {
            positions,
            sampling_rate,
        }
    }

    /// Finds the search start position for the `rank`-th 1.
    /// 查找第 `rank` 个 1 的搜索起始位置。
    #[inline]
    pub fn get_search_start(&self, rank: usize) -> usize {
        // SAFETY: vector indexing is hot, but we should verify bounds or trust logic.
        // Usually safe if rank is valid. To be super robust we usually use get().unwrap_or.
        // But for performance `get_unchecked` is preferred IF we trust the caller.
        // We will keep safe `get` here as it's just one lookup per select.
        let idx = rank / self.sampling_rate;
        // Optimized: `positions` length is sufficient for all valid ranks.
        // 优化：`positions` 长度足以容纳所有有效 rank。
        if idx < self.positions.len() {
             unsafe { *self.positions.get_unchecked(idx) }
        } else {
             0
        }
    }

    /// Gets the starting rank for the current sampling interval.
    /// 获取当前采样区间的起始 rank。
    #[inline]
    pub fn get_rank_start(&self, rank: usize) -> usize {
        (rank / self.sampling_rate) * self.sampling_rate
    }
}

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
    /// Construct a new Elias-Fano index from sorted values.
    /// 从排序值构建新的 Elias-Fano 索引。
    pub fn new(values: &[u64]) -> Self {
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

        // Sampling rate 64 for O(1) select access optimization
        // 采样率 64，用于 O(1) select 访问优化
        let select_index = Sel::new(&high_bits, n, 64);

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
        let bv = Bv::new();
        Self {
            high_bits: bv.clone(),
            low_bits: bv.clone(),
            select_index: Sel {
                positions: vec![],
                sampling_rate: 1,
            },
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
        let mut pos = self.select_index.get_search_start(rank);
        let mut current_rank = self.select_index.get_rank_start(rank);

        // Hot loop: scan for 1s
        // 热循环：扫描 1
        loop {
            // SAFETY: We assume valid construction means we surely find the rank-th 1.
            // 安全性：假设构造有效，必须要找到第 rank 个 1。
            if unsafe { self.high_bits.get_unchecked(pos) } {
                if current_rank == rank {
                    return pos;
                }
                current_rank += 1;
            }
            pos += 1;
        }
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
        let low_val = unsafe { self.low_bits.get_int_unchecked(index * self.low_len, self.low_len) };

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
             unsafe {
                 Some((low, self.get(low).unwrap_unchecked()))
             }
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
        self.high_bits.size_in_bytes()
            + self.low_bits.size_in_bytes()
            + (self.select_index.positions.len() * 8)
    }
}
