use jdb_val::{Flag, Head, Loc};
use zerocopy::IntoBytes;

#[test]
fn both_inline() {
  let key = b"hello";
  let val = b"world";
  let head = Head::both_inline(key, val).unwrap();

  assert!(head.key_flag.is_inline());
  assert!(head.val_flag.is_inline());
  assert_eq!(head.key_len.get(), 5);
  assert_eq!(head.val_len.get(), 5);
  assert_eq!(head.key_data(), key);
  assert_eq!(head.val_data(), val);
  assert_ne!(head.head_crc32.get(), 0);
}

#[test]
fn key_inline() {
  let key = b"mykey";
  let val_loc = Loc::new(1, 100);
  let val_crc32 = 0x12345678;
  let head = Head::key_inline(key, Flag::FILE, val_loc, 1024, val_crc32).unwrap();

  assert!(head.key_flag.is_inline());
  assert!(!head.val_flag.is_inline());
  assert_eq!(head.key_len.get(), 5);
  assert_eq!(head.val_len.get(), 1024);
  assert_eq!(head.key_data(), key);
  let loc = head.val_loc();
  assert_eq!(loc.id(), 1);
  assert_eq!(loc.pos(), 100);
  assert_eq!(head.val_crc32(), val_crc32);
}

#[test]
fn val_inline() {
  let key_loc = Loc::new(2, 200);
  let val = b"short_value";
  let head = Head::val_inline(Flag::FILE, key_loc, 128, val).unwrap();

  assert!(!head.key_flag.is_inline());
  assert!(head.val_flag.is_inline());
  assert_eq!(head.key_len.get(), 128);
  assert_eq!(head.val_len.get(), val.len() as u32);
  let loc = head.key_loc();
  assert_eq!(loc.id(), 2);
  assert_eq!(loc.pos(), 200);
  assert_eq!(head.val_data(), val);
}

#[test]
fn both_file() {
  let key_loc = Loc::new(3, 300);
  let val_loc = Loc::new(4, 400);
  let val_crc32 = 0xDEADBEEF;
  let head = Head::both_file(Flag::FILE, key_loc, 256, Flag::FILE, val_loc, 4096, val_crc32).unwrap();

  assert!(!head.key_flag.is_inline());
  assert!(!head.val_flag.is_inline());
  assert_eq!(head.key_len.get(), 256);
  assert_eq!(head.val_len.get(), 4096);
  let kloc = head.key_loc();
  assert_eq!(kloc.id(), 3);
  assert_eq!(kloc.pos(), 300);
  let vloc = head.val_loc();
  assert_eq!(vloc.id(), 4);
  assert_eq!(vloc.pos(), 400);
  assert_eq!(head.val_crc32(), val_crc32);
}

#[test]
fn max_both_inline() {
  let key = [0u8; 26];
  let val = [1u8; 26];
  let head = Head::both_inline(&key, &val).unwrap();
  assert_eq!(head.key_data(), &key);
  assert_eq!(head.val_data(), &val);
}

#[test]
fn crc_verify() {
  let head = Head::both_inline(b"k", b"v").unwrap();
  let bytes = head.as_bytes();
  let crc = crc32fast::hash(&bytes[..60]);
  assert_eq!(head.head_crc32.get(), crc);
}

#[test]
fn overflow() {
  let key = [0u8; 30];
  let val = [0u8; 30];
  // 30 + 30 = 60 > 52
  assert!(Head::both_inline(&key, &val).is_err());
}

#[test]
fn invalid_flag() {
  // key_inline with inline val_flag should fail
  let key = b"test";
  let val_loc = Loc::new(1, 1);
  assert!(Head::key_inline(key, Flag::INLINE, val_loc, 100, 0).is_err());

  // val_inline with inline key_flag should fail
  let val = b"test";
  let key_loc = Loc::new(1, 1);
  assert!(Head::val_inline(Flag::INLINE, key_loc, 100, val).is_err());
}
