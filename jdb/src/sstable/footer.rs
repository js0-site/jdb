//! SSTable footer with PGM-index support
//! 支持 PGM 索引的 SSTable 尾部
//!
//! Fixed-size footer at end of SSTable file.
//! SSTable 文件末尾的固定大小尾部。

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Footer size in bytes (44 bytes)
/// 尾部大小（44字节）
pub const FOOTER_SIZE: usize = 44;

/// Footer builder
/// 尾部构建器
pub struct FooterBuilder {
  pub filter_offset: u64,
  pub filter_size: u32,
  pub offsets_offset: u64,
  pub offsets_size: u32,
  pub pgm_offset: u64,
  pub pgm_size: u32,
  pub block_count: u32,
  pub checksum: u32,
}

impl FooterBuilder {
  #[inline]
  pub fn build(self) -> Footer {
    Footer {
      filter_offset: self.filter_offset,
      filter_size: self.filter_size,
      offsets_offset: self.offsets_offset,
      offsets_size: self.offsets_size,
      pgm_offset: self.pgm_offset,
      pgm_size: self.pgm_size,
      block_count: self.block_count,
      checksum: self.checksum,
    }
  }
}

/// SSTable footer (44 bytes, packed)
/// SSTable 尾部（44字节，紧凑）
///
/// Layout (PGM-index version):
/// - filter_offset: u64 - BinaryFuse8 filter offset
/// - filter_size: u32 - BinaryFuse8 filter size
/// - offsets_offset: u64 - Block offset array position
/// - offsets_size: u32 - Block offset array size
/// - pgm_offset: u64 - PGM index offset
/// - pgm_size: u32 - PGM index size
/// - block_count: u32 - Number of data blocks
/// - checksum: u32 - CRC32 checksum
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Footer {
  filter_offset: u64,
  filter_size: u32,
  offsets_offset: u64,
  offsets_size: u32,
  pgm_offset: u64,
  pgm_size: u32,
  block_count: u32,
  checksum: u32,
}

impl Footer {
  #[inline]
  pub fn filter_offset(&self) -> u64 {
    self.filter_offset
  }

  #[inline]
  pub fn filter_size(&self) -> u32 {
    self.filter_size
  }

  #[inline]
  pub fn offsets_offset(&self) -> u64 {
    self.offsets_offset
  }

  #[inline]
  pub fn offsets_size(&self) -> u32 {
    self.offsets_size
  }

  #[inline]
  pub fn pgm_offset(&self) -> u64 {
    self.pgm_offset
  }

  #[inline]
  pub fn pgm_size(&self) -> u32 {
    self.pgm_size
  }

  #[inline]
  pub fn block_count(&self) -> u32 {
    self.block_count
  }

  #[inline]
  pub fn checksum(&self) -> u32 {
    self.checksum
  }
}

const _: () = assert!(size_of::<Footer>() == FOOTER_SIZE);
