//! Item encoding and decoding
//! 条目编码和解码
use zbin::Bin;

mod decode;
mod encode;

pub use decode::{Decode, Error, ParseResult, Result};
pub use encode::Encode;

/// CRC32 checksum size
/// CRC32 校验和大小
pub const CRC_SIZE: usize = 4;

/// Magic byte size
/// 魔数字节大小
pub const MAGIC_SIZE: usize = 1;

/// Item trait for log-structured data
/// 日志结构数据的 Item trait
pub trait Item {
  const MAGIC: u8;
  const LEN_BYTES: usize = 1;
  type Data<'a>: Bin<'a>
  where
    Self: 'a;

  /// Get data length from len bytes
  /// 从长度字节获取数据长度
  fn len(byte: &[u8]) -> usize;
}
