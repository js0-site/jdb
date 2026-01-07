//! SSTable foot with PGM-index support
//! 支持 PGM 索引的 SSTable 尾部

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Current SSTable version
/// 当前 SSTable 版本
pub(crate) const VERSION: u8 = 1;

/// Footer size in bytes (43 bytes)
/// 尾部大小（43字节）
pub(crate) const FOOT_SIZE: usize = 43;

/// SSTable foot (43 bytes, packed)
/// SSTable 尾部（43字节，紧凑）
///
/// Layout guarantee: foot always ends with [version: u8, checksum: u32]
/// 布局保证：foot 始终以 [version: u8, checksum: u32] 结尾
///
/// Checksum covers all metadata including version
/// 校验和覆盖所有元数据（包括 version）
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
pub(crate) struct Foot {
  pub filter_offset: u64,
  pub filter_size: u32,
  pub index_size: u32,
  pub offsets_size: u32,
  pub pgm_size: u32,
  pub block_count: u32,
  /// Max version of all entries
  /// 所有条目的最大版本号
  pub max_ver: u64,
  pub prefix_len: u8,
  /// Level number (0 = L0, 1 = L1, ...)
  /// 层级编号
  pub level: u8,
  pub version: u8,
  pub checksum: u32,
}

const _: () = assert!(size_of::<Foot>() == FOOT_SIZE);
