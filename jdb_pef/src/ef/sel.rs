use crate::bits::Bv;

/// Constant-time Select optimized (DArray-like)
/// Uses a simplified DArray approach: Two-level index.
/// 1. Superblocks (L1): Store position of every 1024-th 1.
/// 2. Blocks (L2): Within a superblock, if it's "dense" enough, we just scan (linear scan is fast for small ranges).
///    But wait, DArray creates a sub-index for "sparse" blocks.
///    Here we implement a "Wide-Sampling + Local Offset" approach which is easier but faster than simple sampling.
///
/// Refined Plan:
/// Implement a simplified DArray.
/// - Block size: 1024 ones.
/// - Sub-block size: 32 ones.
/// - If a Block is large (sparse bits), we might need `pos` array.
///
/// Current implementation aims for O(1) select.
/// We stick to the structure: `positions` stores absolute position of every `sampling_rate`-th 1.
/// To make it DArray-like O(1), we need to handle the "in-between" ones.
/// The standard DArray uses a secondary array for offsets relative to the L1 position.
///
/// Let's implement a clean 2-level structure:
/// L1: Absolute position of every `L1_RATE` (e.g. 512) ones.
/// L2: Relative offset of every `L2_RATE` (e.g. 32) ones within the L1 interval.
///      Stored in a tight packed array (e.g. 16 bits or variable).
///
/// For this implementation, to beat the benchmark, we use L1=512, L2=64.
/// And we use `select64` for the final 64 bits.
/// This reduces the scan range to at most 1 word (usually).
///
/// Actually, to get true speed, L1=512, L2=64 is good.
/// Since `select64` instruction is fast, scanning 1 word is basically O(1).
/// The bottleneck was scanning *multiple* words.
/// With sampling 64, we might scan avg 0.5 words IF the density is high.
/// If density is 50%, 64 ones = 128 bits = 2 words.
///
/// We will stick to `Sel` struct but rename internal logic to use this 2-level.
/// Or better: Keep `Sel` struct field but change content.
///
/// positions: Vec<usize> -> L1 samples (every 512th 1).
/// l2_offsets: Vec<u16> -> L2 offsets relative to L1 (every 64th 1).
///
/// Detailed:
/// `rank` -> `l1_idx = rank / 512`. `l1_pos = positions[l1_idx]`.
/// `rem_rank = rank % 512`.
/// `l2_idx = rem_rank / 64`.
/// `l2_pos = l1_pos + l2_offsets[l1_idx * (512/64) + l2_idx]`.
/// `final_rem = rem_rank % 64`.
/// Scan from `l2_pos` for `final_rem` ones.
///
/// Since `final_rem < 64`, and we expect high density in EF (upper bits), 
/// this scan will likely be within 1-2 words.
///
/// For sparse arrays, 64 ones might span many words. DArray handles this by storing explicit positions for sparse blocks.
/// But for EF `high_bits`, the density is related to `u/n`.
/// Usually `high_bits` is dense (unary codes). Avg density is 2 bits per 1 (for u~n).
/// So 64 ones ~ 128 bits ~ 2 words.
/// scanning 2 words is very fast with `select64`.
///
/// So the plan:
/// 1. L1 Sample: Every 512 ones. (u64 absolute)
/// 2. L2 Sample: Every 64 ones, relative to L1. (u16 relative)
///    If relative offset > 65535, we fallback? or use u32?
///    For EF high_bits, `u/n` is usually small. Gaps are small. u16 (65536 bits) is huge gap. safe.
///
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct Sel {
    // L1: Absolute position of every 512-th 1
    pub(crate) l1_positions: Vec<usize>,
    // L2: Relative position of every 64-th 1 within L1 block
    // Stored as u16 to save space. 
    // Layout: For each L1 block, we have (512/64) = 8 L2 entries.
    // entry 0 is always 0 (redundant but simplifies logic).
    pub(crate) l2_offsets: Vec<u16>,
    
    // Config
    pub(crate) l1_rate: usize, // 512
    pub(crate) l2_rate: usize, // 64
}

impl Default for Sel {
    fn default() -> Self {
        Self {
            l1_positions: vec![],
            l2_offsets: vec![],
            l1_rate: crate::conf::DEFAULT_L1_RATE,
            l2_rate: crate::conf::DEFAULT_L2_RATE,
        }
    }
}

impl Sel {
    pub fn new(bv: &Bv, n_ones: usize, conf: crate::conf::Conf) -> Self {
        let l1_rate = conf.l1_rate;
        let l2_rate = conf.l2_rate;

        if n_ones == 0 || l1_rate == 0 || l2_rate == 0 {
            return Self {
                l1_positions: vec![],
                l2_offsets: vec![],
                l1_rate,
                l2_rate,
            };
        }

        let mut l1_positions = Vec::with_capacity(n_ones / l1_rate + 1);
        let mut l2_offsets = Vec::with_capacity(n_ones / l2_rate + 1); 

        let mut ones_count = 0;
        let mut total_bits = 0;
        
        // Re-implement construction safely
        let mut l1_base = 0;
        
        for &w in &bv.data {
             let mut temp = w;
             // Process all ones in this word
             while temp != 0 {
                 let t = temp.trailing_zeros() as usize; // index of LSB 1
                 // Absolute position: total_bits + t
                 let abs_pos = total_bits + t;
                 
                 if ones_count % l1_rate == 0 {
                     l1_positions.push(abs_pos);
                     l1_base = abs_pos;
                     l2_offsets.push(0);
                 } else if ones_count % l2_rate == 0 {
                     let rel = abs_pos - l1_base;
                     if rel > u16::MAX as usize {
                         l2_offsets.push(u16::MAX);
                     } else {
                         l2_offsets.push(rel as u16);
                     }
                 }
                 
                 temp &= temp - 1;
                 ones_count += 1;
             }
             total_bits += 64;
        }

        l1_positions.shrink_to_fit();
        l2_offsets.shrink_to_fit();

        Self {
            l1_positions,
            l2_offsets,
            l1_rate,
            l2_rate,
        }
    }

    #[inline(always)]
    pub fn get_search_start(&self, rank: usize) -> usize {
        // rank is 0-indexed count of 1s
        let l1_idx = rank / self.l1_rate;
        if l1_idx >= self.l1_positions.len() { return 0; } // Should not happen if rank valid
        
        let l1_pos = unsafe { *self.l1_positions.get_unchecked(l1_idx) };
        
        let rem = rank % self.l1_rate;
        let _l2_idx_in_block = rem / self.l2_rate;
        
        // l2_offsets is flat.
        // Index = (rank / l2_rate) ?
        // Yes, because we push for every l2_rate.
        // l2_global_idx = rank / l2_rate.
        let l2_global_idx = rank / self.l2_rate;
        
        if l2_global_idx >= self.l2_offsets.len() { return l1_pos; }
        
        let rel = unsafe { *self.l2_offsets.get_unchecked(l2_global_idx) };
        
        if rel == u16::MAX {
             // Fallback: Overflowed relative offset. 
             // Just return l1_pos. The scan will be longer but correct.
             return l1_pos;
        }
        
        l1_pos + rel as usize
    }

    #[inline(always)]
    pub fn get_rank_start(&self, rank: usize) -> usize {
        // The rank of the 1 at `get_search_start`
        // We jumped to the position of (rank / l2_rate * l2_rate)-th 1.
        (rank / self.l2_rate) * self.l2_rate
    }
}
