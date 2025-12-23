//! Hash algorithm wrapper 哈希算法封装
//! SIMD accelerated via gxhash 通过 gxhash 实现 SIMD 加速

use crate::VNodeID;
use gxhash::{gxhash128, gxhash64};

/// 64-bit hash (for TableID, routing) 64 位哈希（用于 TableID、路由）
#[inline(always)]
pub fn fast_hash64(data: &[u8]) -> u64 {
  gxhash64(data, 0) // seed=0 for determinism 种子=0 保证确定性
}

/// 128-bit hash (for large scale collision reduction) 128 位哈希（减少大规模碰撞）
#[inline(always)]
pub fn fast_hash128(data: &[u8]) -> u128 {
  gxhash128(data, 0)
}

/// Route key to VNode 路由 key 到 VNode
#[inline]
pub fn route_to_vnode(key_hash: u64, total: u16) -> VNodeID {
  debug_assert!(total > 0, "total must be > 0");
  VNodeID((key_hash % u64::from(total)) as u16)
}
