//! CRC32 校验 CRC32 checksum

/// 计算 CRC32 Compute CRC32
#[inline(always)]
pub fn crc32(data: &[u8]) -> u32 {
  crc32fast::hash(data)
}

/// 验证 CRC32 Verify CRC32
#[inline(always)]
pub fn verify(data: &[u8], expected: u32) -> bool {
  crc32(data) == expected
}
