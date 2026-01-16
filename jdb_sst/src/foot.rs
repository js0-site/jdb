//! SSTable foot with PGM-index support
//! 支持 PGM 索引的 SSTable 尾部

use zerocopy::{
  FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
  little_endian::{U32, U64},
};

/// SSTable foot
/// SSTable 尾部
///
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
pub struct Foot {
  pub block_count: U32,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
pub struct FootCrc {
  pub foot: Foot,
  /// CRC32 checksum (covering filter + index + foot fields before checksum)
  /// CRC32 校验和 (覆盖 filter + index + 校验和之前的 foot 字段)
  pub checksum: U32,
  pub magic_ver: U64,
}

impl FootCrc {
  pub const SIZE: usize = size_of::<Self>();
}
