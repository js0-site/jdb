//! Core type system 核心类型系统
//! NewType pattern prevents primitive type misuse NewType 模式防止原生类型混用

use bitcode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// 64-bit Table ID (generated from table name hash)
/// 64 位表 ID（由表名哈希生成）
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode,
)]
#[repr(transparent)]
pub struct TableID(pub u64);

impl TableID {
  #[inline]
  pub const fn new(id: u64) -> Self {
    Self(id)
  }

  /// Generate TableID from binary name 从二进制名称生成 TableID
  #[inline]
  pub fn from_name(name: &[u8]) -> Self {
    Self(crate::fast_hash64(name))
  }
}

/// 32-bit physical page number (supports up to 16TB single file)
/// 32 位物理页号（最大支持 16TB 单文件）
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode,
)]
#[repr(transparent)]
pub struct PageID(pub u32);

impl PageID {
  #[inline]
  pub const fn new(id: u32) -> Self {
    Self(id)
  }

  #[inline]
  pub const fn is_invalid(&self) -> bool {
    self.0 == crate::INVALID_PAGE_ID
  }
}

/// 16-bit virtual node ID (for sharding)
/// 16 位虚拟节点 ID（用于分片路由）
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode,
)]
#[repr(transparent)]
pub struct VNodeID(pub u16);

impl VNodeID {
  #[inline]
  pub const fn new(id: u16) -> Self {
    Self(id)
  }
}

/// 64-bit second timestamp
/// 64 位秒级时间戳
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode,
)]
#[repr(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
  #[inline]
  pub const fn new(ts: u64) -> Self {
    Self(ts)
  }

  /// Get current timestamp in seconds (fast, ~10ns) 获取当前秒级时间戳（快速，约10ns）
  #[inline]
  pub fn now() -> Self {
    Self(coarsetime::Clock::now_since_epoch().as_secs())
  }
}

/// Log Sequence Number (WAL sequence)
/// 日志序列号（WAL 序列）
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode,
)]
#[repr(transparent)]
pub struct Lsn(pub u64);

impl Lsn {
  #[inline]
  pub const fn new(lsn: u64) -> Self {
    Self(lsn)
  }

  #[inline]
  pub const fn next(&self) -> Self {
    Self(self.0 + 1)
  }
}
