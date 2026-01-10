use zbin::Bin;

use crate::encode;

/// CRC32 checksum size
/// CRC32 校验和大小
pub const CRC_SIZE: usize = 4;

/// Parse result
/// 解析结果
pub enum ParseResult<T> {
  /// Successfully parsed
  /// 解析成功
  Ok(T, usize),
  /// Need more bytes
  /// 需要更多字节
  NeedMore,
  /// CRC error or magic mismatch, skip to next magic
  /// CRC 错误或魔数不匹配，跳到下一个魔数
  Corrupted(usize),
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

// 布局  magic len(长度为LEN_BYTES)  data(长度为len()返回值) crc32fast
pub trait Parse {
  const MAGIC: u8;
  const LEN_BYTES: usize;
  type Item<'a>: Bin<'a>
  where
    Self: 'a;

  /// Get data length from len bytes
  /// 从长度字节获取数据长度
  fn len(byte: &[u8]) -> usize;

  /// Convert data length to len bytes
  /// 将数据长度转换为长度字节
  fn len_bytes(len: usize) -> Vec<u8>;

  fn parse_item(bin: &[u8]) -> Option<Self::Item<'_>>;

  /// Encode data to parse format
  /// 将数据编码为 parse 格式
  fn encode(data: &[u8]) -> Vec<u8> {
    encode(Self::MAGIC, &Self::len_bytes(data.len()), data)
  }

  /// Parse and verify item from buffer
  /// 从缓冲区解析并验证条目
  fn parse(bin: &[u8]) -> ParseResult<Self::Item<'_>> {
    let header_size = 1 + Self::LEN_BYTES;
    if bin.len() < header_size {
      return ParseResult::NeedMore;
    }

    if bin[0] != Self::MAGIC {
      let skip = find_next_magic(Self::MAGIC, bin);
      log::warn!("magic mismatch, skip {skip} bytes");
      return ParseResult::Corrupted(skip);
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
      log::warn!("CRC error, skip {skip} bytes");
      return ParseResult::Corrupted(skip);
    }

    match Self::parse_item(&bin[header_size..payload_end]) {
      Some(item) => ParseResult::Ok(item, total_len),
      None => {
        let skip = find_next_magic(Self::MAGIC, bin);
        log::warn!("parse_item failed, skip {skip} bytes");
        ParseResult::Corrupted(skip)
      }
    }
  }
}
