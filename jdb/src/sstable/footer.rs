//! SSTable footer
//! SSTable 尾部
//!
//! Fixed-size footer at end of SSTable file.
//! SSTable 文件末尾的固定大小尾部。

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Footer size in bytes (36 bytes)
/// 尾部大小（36字节）
pub const FOOTER_SIZE: usize = 36;

/// SSTable footer (36 bytes, packed)
/// SSTable 尾部（36字节，紧凑）
///
/// Layout:
/// - filter_offset: u64 - Cuckoo filter block offset
/// - filter_size: u64 - Cuckoo filter block size
/// - index_offset: u64 - Index block offset
/// - index_size: u64 - Index block size
/// - checksum: u32 - CRC32 checksum of data + filter + index
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Footer {
  filter_offset: u64,
  filter_size: u64,
  index_offset: u64,
  index_size: u64,
  checksum: u32,
}

impl Footer {
  /// Create new footer
  /// 创建新尾部
  #[inline]
  pub fn new(
    filter_offset: u64,
    filter_size: u64,
    index_offset: u64,
    index_size: u64,
    checksum: u32,
  ) -> Self {
    Self {
      filter_offset,
      filter_size,
      index_offset,
      index_size,
      checksum,
    }
  }

  /// Get filter block offset
  /// 获取过滤器块偏移
  #[inline]
  pub fn filter_offset(&self) -> u64 {
    self.filter_offset
  }

  /// Get filter block size
  /// 获取过滤器块大小
  #[inline]
  pub fn filter_size(&self) -> u64 {
    self.filter_size
  }

  /// Get index block offset
  /// 获取索引块偏移
  #[inline]
  pub fn index_offset(&self) -> u64 {
    self.index_offset
  }

  /// Get index block size
  /// 获取索引块大小
  #[inline]
  pub fn index_size(&self) -> u64 {
    self.index_size
  }

  /// Get checksum
  /// 获取校验和
  #[inline]
  pub fn checksum(&self) -> u32 {
    self.checksum
  }
}

const _: () = assert!(size_of::<Footer>() == FOOTER_SIZE);
