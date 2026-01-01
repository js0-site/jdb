//! Checkpoint entry format with zerocopy
//! 检查点条目格式，使用 zerocopy
//!
//! Entry layout: Header(8) + data + crc32(4)
//! 条目布局：Header(8) + data + crc32(4)

use jdb_base::Load;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Entry header size
/// 条目头大小
pub const HEADER_SIZE: usize = 8;

/// Magic byte
/// 魔数
pub const MAGIC: u8 = 0xCE;

/// Entry header
/// 条目头
///
/// Layout (8 bytes):
/// 布局 (8 字节)：
/// - magic: 1 byte (0xCE)
/// - _pad: 3 bytes
/// - len: 4 bytes (data length + crc32, excludes header)
#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Header {
  pub magic: u8,
  pub _pad: [u8; 3],
  pub len: u32,
}

impl Header {
  #[inline]
  pub fn new(len: u32) -> Self {
    Self {
      magic: MAGIC,
      _pad: [0; 3],
      len,
    }
  }

  #[inline]
  pub fn is_valid(&self) -> bool {
    self.magic == MAGIC
  }

  /// Total entry size (header + data + crc32)
  /// 条目总大小
  #[inline]
  pub fn entry_size(&self) -> usize {
    HEADER_SIZE + self.len as usize
  }
}

const _: () = assert!(size_of::<Header>() == HEADER_SIZE);

/// Checkpoint entry type for Load trait
/// 检查点条目类型用于 Load trait
pub struct CkpEntry;

impl Load for CkpEntry {
  const MAGIC: u8 = MAGIC;
  const HEAD_SIZE: usize = HEADER_SIZE;
  // Meta is the data between header and crc32
  // Meta 是 header 和 crc32 之间的数据
  const META_OFFSET: usize = HEADER_SIZE;

  #[inline]
  fn len(buf: &[u8]) -> usize {
    if buf.len() < HEADER_SIZE {
      return 0;
    }
    let Some(header) = Header::read_from_bytes(&buf[..HEADER_SIZE]).ok() else {
      return 0;
    };
    if !header.is_valid() || header.len < 4 {
      return 0;
    }
    header.entry_size()
  }

  #[inline]
  fn crc_offset(len: usize) -> usize {
    len - 4
  }

  #[inline]
  fn meta_len(len: usize) -> usize {
    len - HEADER_SIZE - 4
  }
}

impl CkpEntry {
  /// Get data slice from entry
  /// 获取条目数据切片
  #[inline]
  pub fn data(bin: &[u8]) -> Option<&[u8]> {
    let len = Self::parse(bin);
    if len == 0 {
      return None;
    }
    Some(&bin[HEADER_SIZE..len - 4])
  }
}

/// Build entry bytes: header + data + crc32
/// 构建条目字节：header + data + crc32
#[inline]
pub fn build(data: &[u8]) -> Vec<u8> {
  let header = Header::new(data.len() as u32 + 4); // +4 for crc32
  let crc = crc32fast::hash(data);

  let mut buf = Vec::with_capacity(HEADER_SIZE + data.len() + 4);
  buf.extend_from_slice(header.as_bytes());
  buf.extend_from_slice(data);
  buf.extend_from_slice(&crc.to_le_bytes());
  buf
}

/// Parse entry at position, returns (data_slice, next_pos) or None
/// 解析位置处的条目，返回 (数据切片, 下一位置) 或 None
#[inline]
pub fn parse(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
  let slice = buf.get(pos..)?;
  let data = CkpEntry::data(slice)?;
  let entry_size = CkpEntry::len(slice);
  Some((data, pos + entry_size))
}
