//! Blob protocol 大对象协议
//! Block header for value log 值日志的块头

use bytes::{Buf, BufMut};

/// Blob block header size 块头大小
pub const BLOB_HEADER_SIZE: usize = 16;

/// Blob block header 块头
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct BlobHeader {
  /// Data length 数据长度
  pub len: u32,
  /// Checksum 校验和
  pub checksum: u32,
  /// Timestamp 时间戳
  pub ts: u64,
}

impl BlobHeader {
  /// Read from bytes 从字节读取
  #[inline]
  pub fn read(mut buf: &[u8]) -> Self {
    Self {
      len: buf.get_u32_le(),
      checksum: buf.get_u32_le(),
      ts: buf.get_u64_le(),
    }
  }

  /// Write to bytes 写入字节
  #[inline]
  pub fn write(&self, mut buf: &mut [u8]) {
    buf.put_u32_le(self.len);
    buf.put_u32_le(self.checksum);
    buf.put_u64_le(self.ts);
  }

  /// Create new header 创建新块头
  #[inline]
  pub fn new(len: u32, checksum: u32, ts: u64) -> Self {
    Self { len, checksum, ts }
  }
}

/// Blob pointer (stored in B+ tree leaf) 大对象指针（存储在 B+ 树叶子节点）
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct BlobPtr {
  /// File ID 文件 ID
  pub file_id: u32,
  /// Offset in file 文件内偏移
  pub offset: u64,
  /// Data length 数据长度
  pub len: u32,
}

impl BlobPtr {
  /// Size in bytes 字节大小
  pub const SIZE: usize = 16;

  /// Read from bytes 从字节读取
  #[inline]
  pub fn read(mut buf: &[u8]) -> Self {
    Self {
      file_id: buf.get_u32_le(),
      offset: buf.get_u64_le(),
      len: buf.get_u32_le(),
    }
  }

  /// Write to bytes 写入字节
  #[inline]
  pub fn write(&self, mut buf: &mut [u8]) {
    buf.put_u32_le(self.file_id);
    buf.put_u64_le(self.offset);
    buf.put_u32_le(self.len);
  }

  /// Create new pointer 创建新指针
  #[inline]
  pub fn new(file_id: u32, offset: u64, len: u32) -> Self {
    Self { file_id, offset, len }
  }
}
