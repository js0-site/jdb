//! Head tests
//! 头测试

use jdb_base::{Flag, Pos};
use jdb_fs::head::{HEAD_CRC, HEAD_TOTAL, Head, HeadBuilder, MAGIC};

#[test]
fn test_infile() {
  let mut builder = HeadBuilder::new();
  let key = b"hello";
  let val = b"world";

  let record = builder.infile(1, Flag::INFILE, val, key);
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert_eq!(head.id, 1);
  assert!(!head.is_tombstone());
  assert!(head.val_is_infile());
  assert_eq!(head.key_len, 5);
  assert_eq!(head.val_len, 5);
  assert_eq!(head.key_data(&record[1..]), key);
  assert_eq!(head.val_data(&record[1..]), val);
  assert_eq!(head.record_size(), record.len() - 1);
}

#[test]
fn test_file() {
  let mut builder = HeadBuilder::new();
  let key = b"mykey";
  let val_file_id = 100u64;
  let val_len = 200u32;

  let record = builder.file(2, Flag::FILE, val_file_id, val_len, key);
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert_eq!(head.id, 2);
  assert!(!head.val_is_infile());
  assert!(head.flag().is_file());
  assert_eq!(head.key_len, 5);
  assert_eq!(head.val_len, val_len);
  assert_eq!(head.val_file_id, val_file_id);
  assert_eq!(head.key_data(&record[1..]), key);
  assert_eq!(head.record_size(), record.len() - 1);
}

#[test]
fn test_tombstone_infile() {
  let mut builder = HeadBuilder::new();
  let key = b"deleted_key";
  let old_pos = Pos::new(1, Flag::INFILE, 100, 200, 50);

  let record = builder.tombstone(5, old_pos, key);
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert_eq!(head.id, 5);
  assert!(head.is_tombstone());
  assert!(head.flag().is_infile());
  assert_eq!(head.key_len as usize, key.len());
  assert_eq!(head.val_len, 50);
  assert_eq!(head.key_data(&record[1..]), key);
}

#[test]
fn test_tombstone_file() {
  let mut builder = HeadBuilder::new();
  let key = b"deleted_key";
  let old_pos = Pos::new(1, Flag::FILE_LZ4, 100, 999, 1000);

  let record = builder.tombstone(5, old_pos, key);
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert!(head.is_tombstone());
  assert!(head.flag().is_file());
  assert!(head.flag().is_lz4());
  assert_eq!(head.val_len, 1000);
  assert_eq!(head.val_file_id, 999);
}

#[test]
fn test_magic() {
  let mut builder = HeadBuilder::new();
  let record = builder.infile(1, Flag::INFILE, b"v", b"k");
  assert_eq!(record[0], MAGIC);
}

#[test]
fn test_invalid_magic() {
  let mut builder = HeadBuilder::new();
  let mut record = builder.infile(1, Flag::INFILE, b"v", b"k").to_vec();
  record[0] = 0x00;
  assert!(Head::parse_unchecked(&record).is_none());
}

#[test]
fn test_crc_mismatch() {
  let mut builder = HeadBuilder::new();
  let mut record = builder.infile(1, Flag::INFILE, b"v", b"k").to_vec();
  record[HEAD_TOTAL - 1] ^= 0xFF;
  assert!(Head::parse(&record[1..], 0, 0).is_err());
}

#[test]
fn test_head_size() {
  let mut builder = HeadBuilder::new();

  let record = builder.infile(1, Flag::INFILE, b"value", b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();
  assert_eq!(head.record_size(), HEAD_CRC + 5 + 3);

  let record = builder.file(2, Flag::FILE, 100, 200, b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();
  assert_eq!(head.record_size(), HEAD_CRC + 3);

  let old_pos = Pos::new(1, Flag::INFILE, 1, 1, 100);
  let record = builder.tombstone(3, old_pos, b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();
  assert_eq!(head.record_size(), HEAD_CRC + 3);
}

#[test]
fn test_lz4_flag() {
  let mut builder = HeadBuilder::new();
  let record = builder.infile(1, Flag::INFILE_LZ4, b"compressed", b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert!(head.flag().is_lz4());
  assert!(head.val_is_infile());
}
