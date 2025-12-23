//! Page protocol 页面协议
//! Zero-copy via bytes 通过 bytes 实现零拷贝

use bytes::{Buf, BufMut};
use jdb_comm::{Lsn, PageID, PAGE_HEADER_SIZE, PAGE_SIZE};

/// Page header (32 bytes) 页头（32 字节）
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct PageHeader {
  /// Page ID 页 ID
  pub id: u32,
  /// Page type 页类型
  pub typ: u8,
  /// Flags 标志位
  pub flags: u8,
  /// Reserved 保留
  pub _reserved: u16,
  /// LSN for recovery 恢复用 LSN
  pub lsn: u64,
  /// Item count 条目数
  pub count: u16,
  /// Free space offset 空闲空间偏移
  pub free_off: u16,
  /// Checksum 校验和
  pub checksum: u32,
  /// Padding 填充
  pub _pad: [u8; 8],
}

/// Page type 页类型
pub mod page_type {
  pub const LEAF: u8 = 1;
  pub const INTERNAL: u8 = 2;
  pub const OVERFLOW: u8 = 3;
}

impl PageHeader {
  /// Payload capacity 有效载荷容量
  pub const PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

  /// Read header from bytes 从字节读取页头
  #[inline]
  pub fn read(mut buf: &[u8]) -> Self {
    Self {
      id: buf.get_u32_le(),
      typ: buf.get_u8(),
      flags: buf.get_u8(),
      _reserved: buf.get_u16_le(),
      lsn: buf.get_u64_le(),
      count: buf.get_u16_le(),
      free_off: buf.get_u16_le(),
      checksum: buf.get_u32_le(),
      _pad: {
        let mut p = [0u8; 8];
        buf.copy_to_slice(&mut p);
        p
      },
    }
  }

  /// Write header to bytes 写入页头到字节
  #[inline]
  pub fn write(&self, mut buf: &mut [u8]) {
    buf.put_u32_le(self.id);
    buf.put_u8(self.typ);
    buf.put_u8(self.flags);
    buf.put_u16_le(self._reserved);
    buf.put_u64_le(self.lsn);
    buf.put_u16_le(self.count);
    buf.put_u16_le(self.free_off);
    buf.put_u32_le(self.checksum);
    buf.put_slice(&self._pad);
  }

  /// Create new header 创建新页头
  #[inline]
  pub fn new(id: PageID, typ: u8, lsn: Lsn) -> Self {
    Self {
      id: id.0,
      typ,
      flags: 0,
      _reserved: 0,
      lsn: lsn.0,
      count: 0,
      free_off: PAGE_HEADER_SIZE as u16,
      checksum: 0,
      _pad: [0; 8],
    }
  }
}
