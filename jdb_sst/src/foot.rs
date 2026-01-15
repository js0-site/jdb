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
  /// Filter data offset in file
  /// 文件中 filter 数据的偏移量
  pub filter_offset: U64,
  /// Filter data size
  /// filter 数据大小
  pub filter_size: U32,
  /// Index data size (total size of keys + offsets + pgm)
  /// index 数据总大小
  pub index_size: U32,
  /// Offsets data size
  /// offsets 数据大小
  pub offsets_size: U32,
  /// PGM index data size
  /// PGM索引 数据大小
  pub pgm_size: U32,
  /// Data block count
  /// 数据块数量
  pub block_count: U32,
  /// Common prefix length for PGM
  /// PGM 的公共前缀长度
  pub prefix_len: u8,
  /// Compression: 0=None, 1=LZ4, 2=ZSTD
  /// 压缩算法
  pub compress: u8,
  /// Compression dictionary ID (0 if none)
  /// 压缩字典 ID（无字典则为 0）
  pub compress_dict_id: U64,
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
