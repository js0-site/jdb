//! WAL end marker module tests / WAL 尾部标记模块测试

use jdb_val::{build_end, parse_end};

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
