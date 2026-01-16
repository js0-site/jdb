use crate::CODE_MAX;

const N: usize = CODE_MAX as usize;

/// Frequency counters for symbol pairs.
/// 符号对频率计数器。
pub struct Counters {
  pub count1: [u16; N],
  // Flattened 2D array for better cache locality
  // 扁平化二维数组，提升缓存局部性
  pub count2: Box<[u16; N * N]>,
}

impl Counters {
  pub fn new() -> Self {
    Self {
      count1: [0; N],
      // SAFETY: vec length equals N * N
      // 安全性：vec 长度等于 N * N
      count2: unsafe {
        vec![0; N * N]
          .into_boxed_slice()
          .try_into()
          .unwrap_unchecked()
      },
    }
  }

  #[inline]
  pub fn count1_set(&mut self, pos1: usize, val: u16) {
    debug_assert!(pos1 < N);
    // SAFETY: pos1 < CODE_MAX is guaranteed by caller
    // 安全性：调用者保证 pos1 < CODE_MAX
    unsafe { *self.count1.get_unchecked_mut(pos1) = val };
  }

  #[inline]
  pub fn count1_inc(&mut self, pos1: u16) {
    let i = pos1 as usize;
    debug_assert!(i < N);
    // SAFETY: pos1 is u16 code which is < CODE_MAX
    // 安全性：pos1 是 u16 编码，小于 CODE_MAX
    unsafe {
      let v = self.count1.get_unchecked_mut(i);
      *v = v.saturating_add(1);
    }
  }

  #[inline]
  pub fn count2_inc(&mut self, pos1: usize, pos2: usize) {
    debug_assert!(pos1 < N && pos2 < N);
    // SAFETY: pos1, pos2 < CODE_MAX is guaranteed
    // 安全性：pos1, pos2 < CODE_MAX 由调用者保证
    unsafe {
      let v = self.count2.get_unchecked_mut(pos1 * N + pos2);
      *v = v.saturating_add(1);
    }
  }

  #[inline]
  pub fn count1_get(&self, pos1: usize) -> u16 {
    debug_assert!(pos1 < N);
    // SAFETY: pos1 < CODE_MAX is guaranteed
    // 安全性：pos1 < CODE_MAX 由调用者保证
    unsafe { *self.count1.get_unchecked(pos1) }
  }

  #[inline]
  pub fn count2_get(&self, pos1: usize, pos2: usize) -> u16 {
    debug_assert!(pos1 < N && pos2 < N);
    // SAFETY: pos1, pos2 < CODE_MAX is guaranteed
    // 安全性：pos1, pos2 < CODE_MAX 由调用者保证
    unsafe { *self.count2.get_unchecked(pos1 * N + pos2) }
  }
}

impl Default for Counters {
  fn default() -> Self {
    Self::new()
  }
}
