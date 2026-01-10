//! Decode trait for reading items
//! 用于读取条目的 Decode trait

use std::io;

use thiserror::Error;

use super::{CRC_SIZE, Item};

/// Database error type
/// 数据库错误类型
#[derive(Debug, Error)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] io::Error),

  #[error("Data corrupted: {0}")]
  Corrupted(String),

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

/// Decode trait for parsing items from buffer
/// 用于从缓冲区解析条目的 Decode trait
pub trait Decode: Item {
  /// Parse item from data bytes
  /// 从数据字节解析条目
  fn decode_item(bin: &[u8]) -> Option<Self::Data<'_>>;

  /// Parse and verify item from buffer
  /// 从缓冲区解析并验证条目
  fn decode(bin: &[u8]) -> ParseResult<Self::Data<'_>> {
    let header_size = 1 + Self::LEN_BYTES;
    if bin.len() < header_size {
      return ParseResult::NeedMore;
    }

    if bin[0] != Self::MAGIC {
      let skip = find_next_magic(Self::MAGIC, bin);
      return ParseResult::Err(Error::MagicMismatch, skip);
    }

    let data_len = Self::len(&bin[1..]);
    let total_len = header_size + data_len + CRC_SIZE;
    if bin.len() < total_len {
      return ParseResult::NeedMore;
    }

    // Verify CRC32
    // 验证 CRC32
    let payload_end = header_size + data_len;
    let payload = &bin[..payload_end];
    let crc_bytes = &bin[payload_end..total_len];
    let expected = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    if crc32fast::hash(payload) != expected {
      let skip = find_next_magic(Self::MAGIC, bin);
      return ParseResult::Err(Error::CrcMismatch, skip);
    }

    match Self::decode_item(&bin[header_size..payload_end]) {
      Some(item) => ParseResult::Ok(item, total_len),
      None => {
        let skip = find_next_magic(Self::MAGIC, bin);
        ParseResult::Err(Error::Corrupted("Item decode failed".into()), skip)
      }
    }
  }
}
