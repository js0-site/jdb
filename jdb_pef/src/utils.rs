/// Find the bit-index of the k-th set bit in a 64-bit word.
/// 在 64 位字中查找第 k 个置位位的位索引。
/// k is 0-indexed (0 means first set bit).
/// k 从 0 开始索引（0 表示第一个置位位）。
/// Returns 64 if not found (k >= population).
/// 如果未找到则返回 64（k >= 置位数）。
#[inline(always)]
pub fn select64(mut word: u64, k: usize) -> usize {
  #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
  {
    // BMI2 PDEP approach:
    // BMI2 PDEP 方法：
    // 1 << k creates a mask with k-th bit set.
    // 1 << k 创建一个第 k 位置位的掩码。
    // PDEP maps this bit to the k-th set bit in word.
    // PDEP 将此位映射到 word 中第 k 个置位位。
    // Requires k < count_ones.
    // 要求 k < 置位数。
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
    // Fallback: loop clearing lowest bit.
    // 回退方案：循环清除最低位。

    // Early check: k >= popcount means not found
    // 提前检查：k >= popcount 表示未找到
    if k >= word.count_ones() as usize {
      return 64;
    }

    // Clear lowest k set bits, then find the (k+1)-th one
    // 清除最低的 k 个设置位，然后找到第 (k+1) 个
    for _ in 0..k {
      word &= word - 1;
    }
    word.trailing_zeros() as usize
  }
}
