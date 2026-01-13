//! SSTable foot with PGM-index support
//! 支持 PGM 索引的 SSTable 尾部

use zerocopy::{
  FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
  little_endian::{U32, U64},
};

use crate::VER;

/// SSTable foot
/// SSTable 尾部
///
/// Layout guarantee: foot always ends with [version: u8, checksum: U32]
/// 布局保证：foot 始终以 [version: u8, checksum: U32] 结尾
///
/// Checksum covers all metadata including version
/// 校验和覆盖所有元数据（包括 version）
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
pub struct Foot {
  pub filter_offset: U64,
  pub filter_size: U32,
  pub index_size: U32,
  pub offsets_size: U32,
  pub pgm_size: U32,
  pub block_count: U32,
  /// Max version of all entries
  /// 所有条目的最大版本号
  pub max_ver: U64,
  /// Tombstone size (key_len + val_len + overhead)
  /// 墓碑大小（key_len + val_len + 固定开销）
  pub rmed_size: U64,
  pub prefix_len: u8,
  /// Level number (0 = L0, 1 = L1, ...)
  /// 层级编号
  pub level: u8,
  pub version: u8,
  pub checksum: U32,
}

impl Foot {
  pub const SIZE: usize = size_of::<Self>();
}
