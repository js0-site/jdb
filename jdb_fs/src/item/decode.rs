//! Decode function for reading items
//! 用于读取条目的解码函数

use std::mem::size_of;

use zerocopy::{FromBytes, IntoBytes};

use super::{Error, Item, Row};

/// Decode result
/// 解码结果
pub enum ParseResult<T> {
  /// Successfully decoded
  /// 解码成功
  Ok(T),
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
pub fn find_next_magic(magic: u8, bin: &[u8]) -> usize {
  // Use memchr for high-performance byte search
  // 使用 memchr 进行高性能字节搜索
  memchr::memchr(magic, &bin[1..]).map_or(bin.len(), |pos| 1 + pos)
}

/// Parse and verify head from buffer: magic(1) + head + crc32(4)
/// 从缓冲区解析并验证 head: magic(1) + head + crc32(4)
#[inline]
pub fn parse<I: Item>(bin: &[u8]) -> ParseResult<I::Head> {
  let row_size = size_of::<Row<I::Head>>();

  if bin.len() < row_size {
    return ParseResult::NeedMore;
  }

  let Ok((row, _)) = Row::<I::Head>::read_from_prefix(bin) else {
    return ParseResult::Err(Error::Decode, find_next_magic(I::MAGIC, bin));
  };

  if row.magic != I::MAGIC {
    return ParseResult::Err(Error::Magic, find_next_magic(I::MAGIC, bin));
  }

  // Copy head to avoid unaligned access
  // 复制 head 以避免未对齐访问
  let head = row.head;

  // Verify CRC32
  // 验证 CRC32
  if crc32fast::hash(head.as_bytes()) != row.crc32 {
    return ParseResult::Err(Error::Crc, find_next_magic(I::MAGIC, bin));
  }

  ParseResult::Ok(head)
}
