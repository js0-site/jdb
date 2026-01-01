//! SSTable - Sorted String Table
//! 有序字符串表
//!
//! On-disk storage format for LSM-Tree.
//! LSM-Tree 的磁盘存储格式。

mod footer;
mod meta;
mod reader;
mod writer;

pub use footer::{FOOTER_SIZE, Footer, FooterBuilder};
pub use meta::TableMeta;
pub use reader::{SSTableIter, TableInfo};
pub use writer::Writer;

/// Convert key bytes to u64 prefix (big-endian, pad with 0)
/// 将键字节转换为 u64 前缀（大端序，不足补0）
#[inline]
pub fn key_to_u64(key: &[u8]) -> u64 {
  let mut buf = [0u8; 8];
  let len = key.len().min(8);
  buf[..len].copy_from_slice(&key[..len]);
  u64::from_be_bytes(buf)
}
