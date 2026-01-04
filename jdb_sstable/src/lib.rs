#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table
//! 有序字符串表

mod block;
mod error;
mod footer;
mod meta;
mod reader;
mod stream;
mod table;
mod writer;

pub use error::{Error, Result};
pub use meta::TableMeta;
pub use reader::TableInfo;
pub use stream::{AscStream, DescStream};
pub use table::SSTable;
pub use writer::{DEFAULT_BLOCK_SIZE, Writer};

/// Convert key bytes to u64 prefix (big-endian, pad with 0)
/// 将键字节转换为 u64 前缀（大端序，不足补0）
#[inline]
fn key_to_u64(key: &[u8]) -> u64 {
  let mut buf = [0u8; 8];
  let len = key.len().min(8);
  buf[..len].copy_from_slice(&key[..len]);
  u64::from_be_bytes(buf)
}
