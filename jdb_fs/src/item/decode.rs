//! Decode function for reading items
//! 用于读取条目的解码函数

use std::io;

use thiserror::Error;

use super::{CRC_SIZE, Item};

/// Database error type
/// 数据库错误类型
#[derive(Debug, Error)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] io::Error),

  #[error("Decode failed")]
  DecodeFailed,

  #[error("Magic mismatch")]
  MagicMismatch,

  #[error("CRC mismatch")]
  CrcMismatch,
}

/// Result alias
/// 结果别名
pub type Result<T> = std::result::Result<T, Error>;

/// Decode result
/// 解码结果
pub enum ParseResult<T> {
  /// Successfully decoded
  /// 解码成功
  Ok(T, usize),
  /// Need more bytes
  /// 需要更多字节
  NeedMore,
  /// Corrupted data, skip bytes
  /// 损坏数据，跳过字节
  Err(Error, usize),
}

/// Search next magic from position 1, return skip bytes
/// 从位置 1 开始搜索下一个魔数，返回跳过的字节数
#[inline]
fn find_next_magic(magic: u8, bin: &[u8]) -> usize {
  match memchr::memchr(magic, &bin[1..]) {
    Some(pos) => 1 + pos,
    None => bin.len(),
  }
}

/// Parse and verify item from buffer
/// 从缓冲区解析并验证条目
#[inline]
pub fn parse<'a, I: Item, T>(
  bin: &'a [u8],
  decode: impl FnOnce(&'a [u8]) -> Option<T>,
) -> ParseResult<T> {
  let header_size = 1 + I::LEN_BYTES;
  if bin.len() < header_size {
    return ParseResult::NeedMore;
  }

  if bin[0] != I::MAGIC {
    let skip = find_next_magic(I::MAGIC, bin);
    return ParseResult::Err(Error::MagicMismatch, skip);
  }

  let data_len = I::len(&bin[1..]);
  let total_len = header_size + data_len + CRC_SIZE;
  if bin.len() < total_len {
    return ParseResult::NeedMore;
  }

  // Verify CRC32
  // 验证 CRC32
  let payload_end = header_size + data_len;
  let payload = &bin[..payload_end];
  let crc_bytes: [u8; 4] = bin[payload_end..total_len].try_into().unwrap();
  if crc32fast::hash(payload) != u32::from_le_bytes(crc_bytes) {
    let skip = find_next_magic(I::MAGIC, bin);
    return ParseResult::Err(Error::CrcMismatch, skip);
  }

  match decode(&bin[header_size..payload_end]) {
    Some(item) => ParseResult::Ok(item, total_len),
    None => {
      let skip = find_next_magic(I::MAGIC, bin);
      ParseResult::Err(Error::DecodeFailed, skip)
    }
  }
}
