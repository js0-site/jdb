//! 哈希封装 Hash wrapper

use gxhash::{gxhash128, gxhash64};

/// 64 位哈希 64-bit hash
#[inline(always)]
pub fn hash64(data: &[u8]) -> u64 {
  gxhash64(data, 0)
}

/// 128 位哈希 128-bit hash
#[inline(always)]
pub fn hash128(data: &[u8]) -> u128 {
  gxhash128(data, 0)
}

/// 当前秒级时间戳 Current timestamp in seconds
#[inline]
pub fn now_sec() -> u64 {
  coarsetime::Clock::now_since_epoch().as_secs()
}
