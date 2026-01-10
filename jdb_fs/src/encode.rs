//! Encode data to parse format
//! 将数据编码为 parse 格式

use crate::parse::CRC_SIZE;

/// Encode data to parse format: magic + len_bytes + data + crc32
/// 将数据编码为 parse 格式：magic + len_bytes + data + crc32
pub fn encode(magic: u8, len_bytes: &[u8], data: &[u8]) -> Vec<u8> {
  let total = 1 + len_bytes.len() + data.len() + CRC_SIZE;
  let mut out = Vec::with_capacity(total);

  out.push(magic);
  out.extend_from_slice(len_bytes);
  out.extend_from_slice(data);

  let crc = crc32fast::hash(&out);
  out.extend_from_slice(&crc.to_le_bytes());

  out
}
