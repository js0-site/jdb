//! SSTable footer with PGM-index support
//! 支持 PGM 索引的 SSTable 尾部

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Current SSTable version
/// 当前 SSTable 版本
pub(crate) const VERSION: u8 = 1;

/// Footer size in bytes (34 bytes)
/// 尾部大小（34字节）
pub(crate) const FOOTER_SIZE: usize = 34;

/// SSTable footer (34 bytes, packed)
/// SSTable 尾部（34字节，紧凑）
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
pub(crate) struct Footer {
  pub filter_offset: u64,
  pub filter_size: u32,
  pub index_size: u32,
  pub offsets_size: u32,
  pub pgm_size: u32,
  pub block_count: u32,
  pub prefix_len: u8,
  pub checksum: u32,
  pub version: u8,
}

const _: () = assert!(size_of::<Footer>() == FOOTER_SIZE);
