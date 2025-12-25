//! Physical frame header / 物理帧头
//!
//! 12-byte header: CRC32(4) + payload_len(4) + flags(1) + reserved(3)
//!
//! Flags layout / 标志位布局:
//! - bit 0-1: compression type / 压缩类型 (00=none, 01=lz4, 10=zstd)
//! - bit 2-7: reserved / 保留

use crate::{Error, Result};

/// Compression type / 压缩类型
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compress {
  /// No compression / 无压缩
  #[default]
  None = 0,
  /// LZ4 compression (fast) / LZ4 压缩（快速）
  Lz4 = 1,
  /// Zstd compression (high ratio) / Zstd 压缩（高压缩比）
  Zstd = 2,
}

impl Compress {
  /// Parse from flags byte / 从标志字节解析
  #[inline]
  pub const fn from_flags(flags: u8) -> Self {
    match flags & 0x03 {
      1 => Self::Lz4,
      2 => Self::Zstd,
      _ => Self::None,
    }
  }

  /// Check if compressed / 是否压缩
  #[inline]
  pub const fn is_compressed(&self) -> bool {
    !matches!(self, Self::None)
  }
}

/// Physical frame header / 物理帧头
/// Total: 12 bytes, aligned to 4 bytes
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
  /// CRC32 of payload / 载荷校验
  pub crc32: u32,
  /// Payload length / 载荷长度
  pub payload_len: u32,
  /// Flags (bit 0-1: compress type) / 标志位
  pub flags: u8,
}

impl Header {
  /// Header size in bytes / 头部字节大小
  pub const SIZE: usize = 12;

  /// Create new header / 创建新头部
  #[inline]
  pub const fn new(crc32: u32, payload_len: u32, compress: Compress) -> Self {
    Self {
      crc32,
      payload_len,
      flags: compress as u8,
    }
  }

  /// Get compression type / 获取压缩类型
  #[inline]
  pub const fn compress(&self) -> Compress {
    Compress::from_flags(self.flags)
  }

  /// Check if payload is compressed / 检查是否压缩
  #[inline]
  pub const fn is_compressed(&self) -> bool {
    self.flags & 0x03 != 0
  }

  /// Encode header to bytes / 编码为字节
  #[inline]
  pub const fn encode(&self) -> [u8; Self::SIZE] {
    let crc = self.crc32.to_le_bytes();
    let len = self.payload_len.to_le_bytes();
    [
      crc[0], crc[1], crc[2], crc[3], len[0], len[1], len[2], len[3], self.flags, 0, 0, 0,
    ]
  }

  /// Decode header from bytes / 从字节解码
  #[inline]
  pub fn decode(bytes: &[u8]) -> Result<Self> {
    if bytes.len() < Self::SIZE {
      return Err(Error::Serialize(format!(
        "header too short: {} < {}",
        bytes.len(),
        Self::SIZE
      )));
    }
    let crc32 = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let payload_len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let flags = bytes[8];
    Ok(Self {
      crc32,
      payload_len,
      flags,
    })
  }
}
