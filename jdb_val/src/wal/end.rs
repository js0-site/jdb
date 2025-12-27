//! End marker for WAL entries / WAL 条目尾部标记
//!
//! Layout (12 bytes) / 布局（12字节）:
//! [head_offset: u64 LE] [magic: u32 LE]

use super::consts::{END_MAGIC, END_SIZE};

/// Build end marker / 构建尾部标记
#[inline]
pub fn build_end(head_offset: u64) -> [u8; END_SIZE] {
  let mut buf = [0u8; END_SIZE];
  buf[0..8].copy_from_slice(&head_offset.to_le_bytes());
  buf[8..12].copy_from_slice(&END_MAGIC.to_le_bytes());
  buf
}

/// Parse end marker, returns head_offset if valid / 解析尾部标记，有效则返回 head_offset
#[inline]
pub fn parse_end(buf: &[u8]) -> Option<u64> {
  if buf.len() < END_SIZE {
    return None;
  }
  // SAFETY: length checked above / 安全：上方已检查长度
  unsafe {
    let magic = u32::from_le_bytes(*buf.get_unchecked(8..12).as_ptr().cast::<[u8; 4]>());
    if magic != END_MAGIC {
      return None;
    }
    let off = u64::from_le_bytes(*buf.get_unchecked(0..8).as_ptr().cast::<[u8; 8]>());
    Some(off)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_build_parse_roundtrip() {
    let offset = 0x123456789ABCDEF0u64;
    let buf = build_end(offset);
    assert_eq!(parse_end(&buf), Some(offset));
  }

  #[test]
  fn test_parse_invalid_magic() {
    let mut buf = build_end(100);
    buf[8] = 0x00; // corrupt magic
    assert_eq!(parse_end(&buf), None);
  }

  #[test]
  fn test_parse_short_buf() {
    let buf = [0u8; 11];
    assert_eq!(parse_end(&buf), None);
  }

  mod prop {
    use proptest::prelude::*;

    use super::*;

    proptest! {
      #![proptest_config(ProptestConfig::with_cases(100))]

      /// Feature: wal-end-marker, Property 1: End Marker Round-Trip
      /// *For any* valid u64 offset value, calling `build_end(offset)` then
      /// `parse_end(&buf)` SHALL return `Some(offset)` with the original value.
      /// **Validates: Requirements 1.1, 1.2**
      #[test]
      fn prop_end_marker_roundtrip(offset: u64) {
        let buf = build_end(offset);
        let parsed = parse_end(&buf);
        prop_assert_eq!(parsed, Some(offset));
      }
    }
  }
}
