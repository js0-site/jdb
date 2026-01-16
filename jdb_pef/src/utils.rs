
/// Find the bit-index of the k-th set bit in a 64-bit word.
/// k is 0-indexed (0 means first set bit).
/// Returns 64 if not found (k >= population).
#[inline(always)]
pub fn select64(mut word: u64, k: usize) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // BMI2 PDEP approach
        // 1 << k creates a mask with k-th bit set. 
        // PDEP maps this bit to the k-th set bit in word.
        // Then trailing_zeros finds its position.
        // Requires k < count_ones.
        use std::arch::x86_64::_pdep_u64;
        unsafe {
            let mask = 1u64 << k;
            let val = _pdep_u64(mask, word);
            if val == 0 {
                return 64; 
            }
            val.trailing_zeros() as usize
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        // Broadword / Loop fallback
        // Simple SWAR is complex to implement correctly without bugs in one go.
        // Given we are likely on modern hardware or k is small, we can use a loop 
        // clearing lowest bit.
        // Or byte-wise lookup?
        // Let's use the robust loop for now, optimizing for average case.
        // For 'get', k is usually small or random.
        
        // Optimization: check popcount first
        if k >= word.count_ones() as usize {
            return 64;
        }
        
        let _idx = 0;
        // Optimization: skip bytes?
        // Let's keep it simple tight loop.
        for _ in 0..k {
            word &= word - 1; // clear lowest set bit
        }
        word.trailing_zeros() as usize
    }
}
