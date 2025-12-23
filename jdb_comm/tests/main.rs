use jdb_comm::{
  hash64, hash128, now_sec,
  E, R,
  PAGE_SIZE, PAGE_HEADER_SIZE, FILE_MAGIC, INVALID_PAGE_ID, BLOB_THRESHOLD,
};

#[test]
fn test_consts() {
  assert_eq!(PAGE_SIZE, 4096);
  assert_eq!(PAGE_HEADER_SIZE, 32);
  assert_eq!(FILE_MAGIC, 0x4A_44_42_5F_46_49_4C_45);
  assert_eq!(INVALID_PAGE_ID, u32::MAX);
  assert_eq!(BLOB_THRESHOLD, 512);
}

#[test]
fn test_hash64_deterministic() {
  let data = b"hello world";
  let h1 = hash64(data);
  let h2 = hash64(data);
  assert_eq!(h1, h2);
}

#[test]
fn test_hash64_different_input() {
  let h1 = hash64(b"hello");
  let h2 = hash64(b"world");
  assert_ne!(h1, h2);
}

#[test]
fn test_hash128_deterministic() {
  let data = b"hello world";
  let h1 = hash128(data);
  let h2 = hash128(data);
  assert_eq!(h1, h2);
}

#[test]
fn test_hash_empty() {
  // 空数据也应该有确定的哈希值
  let h1 = hash64(b"");
  let h2 = hash64(b"");
  assert_eq!(h1, h2);
}

#[test]
fn test_now_sec() {
  let t1 = now_sec();
  let t2 = now_sec();
  // 时间戳应该是合理的（大于 2020 年）
  assert!(t1 > 1577836800); // 2020-01-01
  // 两次调用应该相近
  assert!(t2 >= t1);
  assert!(t2 - t1 < 2);
}

#[test]
fn test_error_io() {
  let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
  let e: E = io_err.into();
  assert!(matches!(e, E::Io(_)));
  assert!(e.to_string().contains("file not found"));
}

#[test]
fn test_error_checksum() {
  let e = E::Checksum(0x1234, 0x5678);
  let s = e.to_string();
  assert!(s.contains("0x1234"));
  assert!(s.contains("0x5678"));
}

#[test]
fn test_error_other() {
  let e = E::other("custom error");
  assert_eq!(e.to_string(), "custom error");
}

#[test]
fn test_result_type() {
  fn may_fail(ok: bool) -> R<i32> {
    if ok { Ok(42) } else { Err(E::NotFound) }
  }

  assert_eq!(may_fail(true).unwrap(), 42);
  assert!(may_fail(false).is_err());
}
