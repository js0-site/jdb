//! Checkpoint entry format with zerocopy
//! 检查点条目格式，使用 zerocopy
//!
//! Entry layout: Header(8) + data + crc32(4)
//! 条目布局：Header(8) + data + crc32(4)

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
  if slice.len() < HEADER_SIZE {
    return None;
  }

  let header = Header::read_from_bytes(&slice[..HEADER_SIZE]).ok()?;
  if !header.is_valid() || header.len < 4 {
    return None;
  }

  let entry_size = header.entry_size();
  if entry_size > slice.len() {
    return None;
  }

  // Verify crc32
  // 验证 crc32
  let data_end = entry_size - 4;
  let data = &slice[HEADER_SIZE..data_end];
  // Safety: entry_size - 4 bytes checked, slice[data_end..entry_size] is exactly 4 bytes
  // 安全：已检查 entry_size，slice[data_end..entry_size] 正好 4 字节
  let stored_crc = u32::from_le_bytes(unsafe { *(slice.as_ptr().add(data_end) as *const [u8; 4]) });

  if crc32fast::hash(data) == stored_crc {
    Some((data, pos + entry_size))
  } else {
    None
  }
}

const _: () = assert!(size_of::<Header>() == HEADER_SIZE);
