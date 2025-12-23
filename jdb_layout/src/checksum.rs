//! Checksum wrapper 校验和封装
//! PCLMULQDQ accelerated via crc_fast 通过 crc_fast 实现硬件加速

use crc_fast::{checksum, CrcAlgorithm};

/// CRC32C checksum CRC32C 校验和
#[inline]
pub fn crc32(data: &[u8]) -> u32 {
  checksum(CrcAlgorithm::Crc32IsoHdlc, data) as u32
}

/// Verify checksum 验证校验和
#[inline]
pub fn verify(data: &[u8], expected: u32) -> bool {
  crc32(data) == expected
}
