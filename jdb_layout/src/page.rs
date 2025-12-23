//! 页头 Page header

use crate::consts::{PAGE_HEADER_SIZE, PAGE_SIZE};

/// 页类型 Page type
pub mod page_type {
  pub const DATA: u8 = 1;
  pub const INDEX_LEAF: u8 = 2;
  pub const INDEX_INTERNAL: u8 = 3;
  pub const OVERFLOW: u8 = 4;
  pub const META: u8 = 5;
}

/// 页魔数 Page magic
pub const PAGE_MAGIC: u32 = 0x4A_44_42_50; // "JDBP"

/// 页头 32 字节
/// Page header 32 bytes
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PageHeader {
  pub magic: u32,      // 4: 魔数
  pub page_id: u32,    // 4: 页 ID
  pub typ: u8,         // 1: 页类型
  pub flags: u8,       // 1: 标志
  pub count: u16,      // 2: 记录数
  pub free_start: u16, // 2: 空闲起始偏移
  pub free_end: u16,   // 2: 空闲结束偏移
  pub next: u32,       // 4: 下一页 (叶子链表)
  pub checksum: u32,   // 4: CRC32
  pub _pad: [u8; 8],   // 8: 填充到 32 字节
}

const _: () = assert!(size_of::<PageHeader>() == PAGE_HEADER_SIZE);

impl Default for PageHeader {
  fn default() -> Self {
    Self {
      magic: PAGE_MAGIC,
      page_id: 0,
      typ: 0,
      flags: 0,
      count: 0,
      free_start: PAGE_HEADER_SIZE as u16,
      free_end: PAGE_SIZE as u16,
      next: u32::MAX,
      checksum: 0,
      _pad: [0; 8],
    }
  }
}

impl PageHeader {
  pub const SIZE: usize = PAGE_HEADER_SIZE;
  pub const PAYLOAD_SIZE: usize = PAGE_SIZE - Self::SIZE;

  /// 创建新页头 Create new page header
  #[inline]
  pub fn new(page_id: u32, typ: u8) -> Self {
    Self {
      page_id,
      typ,
      ..Default::default()
    }
  }

  /// 空闲空间 Free space
  #[inline]
  pub fn free_space(&self) -> usize {
    (self.free_end - self.free_start) as usize
  }

  /// 从字节解码 Decode from bytes
  pub fn decode(buf: &[u8]) -> Self {
    debug_assert!(buf.len() >= Self::SIZE);
    Self {
      magic: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
      page_id: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
      typ: buf[8],
      flags: buf[9],
      count: u16::from_le_bytes([buf[10], buf[11]]),
      free_start: u16::from_le_bytes([buf[12], buf[13]]),
      free_end: u16::from_le_bytes([buf[14], buf[15]]),
      next: u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]),
      checksum: u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]),
      _pad: [buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31]],
    }
  }

  /// 编码到字节 Encode to bytes
  pub fn encode(&self, buf: &mut [u8]) {
    debug_assert!(buf.len() >= Self::SIZE);
    buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
    buf[4..8].copy_from_slice(&self.page_id.to_le_bytes());
    buf[8] = self.typ;
    buf[9] = self.flags;
    buf[10..12].copy_from_slice(&self.count.to_le_bytes());
    buf[12..14].copy_from_slice(&self.free_start.to_le_bytes());
    buf[14..16].copy_from_slice(&self.free_end.to_le_bytes());
    buf[16..20].copy_from_slice(&self.next.to_le_bytes());
    buf[20..24].copy_from_slice(&self.checksum.to_le_bytes());
    buf[24..32].copy_from_slice(&self._pad);
  }

  /// 验证魔数 Verify magic
  #[inline]
  pub fn is_valid(&self) -> bool {
    self.magic == PAGE_MAGIC
  }
}
