//! End marker for WAL entries / WAL 条目尾部标记
//!
//! Layout (12 bytes) / 布局（12字节）:
//! [head_offset: u64 LE] [magic: u32 LE]

use super::consts::{END_MAGIC, END_SIZE};

/// Build end marker / 构建尾部标记
#[inline(always)]
pub fn build_end(head_offset: u64) -> [u8; END_SIZE] {
  let mut buf = [0u8; END_SIZE];
  buf[0..8].copy_from_slice(&head_offset.to_le_bytes());
  buf[8..12].copy_from_slice(&END_MAGIC.to_le_bytes());
  buf
}

/// Parse end marker, returns head_offset if valid / 解析尾部标记，有效则返回 head_offset
#[inline(always)]
pub fn parse_end(buf: &[u8]) -> Option<u64> {
  if buf.len() < END_SIZE {
    return None;
  }
  // SAFETY: length checked >= END_SIZE (12), so [8..12] and [0..8] are valid
  // 安全性：已检查长度 >= END_SIZE (12)，所以 [8..12] 和 [0..8] 有效
  let magic = u32::from_le_bytes(unsafe { buf.get_unchecked(8..12).try_into().unwrap_unchecked() });
  if magic != END_MAGIC {
    return None;
  }
  let off = u64::from_le_bytes(unsafe { buf.get_unchecked(0..8).try_into().unwrap_unchecked() });
  Some(off)
}
