//! Blob 指针 Blob pointer

/// Blob 指针 16 字节 (重排字段避免 padding)
/// Blob pointer 16 bytes (reorder fields to avoid padding)
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobPtr {
  pub offset: u64,  // 8: 文件内偏移
  pub file_id: u32, // 4: 文件 ID
  pub len: u32,     // 4: 数据长度
}

const _: () = assert!(size_of::<BlobPtr>() == 16);

impl BlobPtr {
  pub const SIZE: usize = 16;
  pub const INVALID: Self = Self { offset: 0, file_id: u32::MAX, len: 0 };

  /// 创建 Create
  #[inline]
  pub const fn new(file_id: u32, offset: u64, len: u32) -> Self {
    Self { offset, file_id, len }
  }

  /// 是否有效 Is valid
  #[inline]
  pub const fn is_valid(&self) -> bool {
    self.file_id != u32::MAX
  }

  /// 从字节解码 Decode from bytes
  #[inline]
  pub fn decode(buf: &[u8]) -> Self {
    debug_assert!(buf.len() >= Self::SIZE);
    Self {
      offset: u64::from_le_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]),
      file_id: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
      len: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
    }
  }

  /// 编码到字节 Encode to bytes
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) {
    debug_assert!(buf.len() >= Self::SIZE);
    buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
    buf[8..12].copy_from_slice(&self.file_id.to_le_bytes());
    buf[12..16].copy_from_slice(&self.len.to_le_bytes());
  }
}
