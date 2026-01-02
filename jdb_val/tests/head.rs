//! Head tests
//! 头测试

use jdb_base::Flag;
use jdb_fs::head::{HEAD_CRC, HEAD_TOTAL, Head, HeadBuilder, MAGIC};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Test infile val
/// 测试 INFILE val
#[test]
fn test_infile() {
  let mut builder = HeadBuilder::new();
  let key = b"hello";
  let val = b"world";

  let record = builder.infile(1, Flag::Infile, val, key);
  // Parse from Head (skip magic)
  // 从 Head 解析（跳过 magic）
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert_eq!(head.id, 1);
  assert!(!head.is_tombstone());
  assert!(head.val_is_infile());
  assert_eq!(head.key_len, 5);
  assert_eq!(head.val_len, 5);

  // Data access also from Head position
  // 数据访问也从 Head 位置开始
  assert_eq!(head.key_data(&record[1..]), key);
  assert_eq!(head.val_data(&record[1..]), val);
  assert_eq!(head.record_size(), record.len() - 1);
}

/// Test file val
/// 测试 FILE val
#[test]
fn test_file() {
  let mut builder = HeadBuilder::new();
  let key = b"mykey";
  let val_file_id = 100u64;
  let val_len = 200u32;

  let record = builder.file(2, Flag::File, val_file_id, val_len, key);
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

/// Test tombstone
/// 测试删除标记
#[test]
fn test_tombstone() {
  let mut builder = HeadBuilder::new();
  let key = b"deleted_key";

  let record = builder.tombstone(5, key);
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert_eq!(head.id, 5);
  assert!(head.is_tombstone());
  assert_eq!(head.key_len as usize, key.len());
  assert_eq!(head.val_len, 0);

  assert_eq!(head.key_data(&record[1..]), key);
  assert_eq!(head.record_size(), record.len() - 1);
}

/// Test magic byte
/// 测试魔数字节
#[test]
fn test_magic() {
  let mut builder = HeadBuilder::new();
  let record = builder.infile(1, Flag::Infile, b"v", b"k");
  assert_eq!(record[0], MAGIC);
}

/// Test invalid magic (parse_unchecked)
/// 测试无效魔数（parse_unchecked）
#[test]
fn test_invalid_magic() {
  let mut builder = HeadBuilder::new();
  let mut record = builder.infile(1, Flag::Infile, b"v", b"k").to_vec();
  record[0] = 0x00;
  // parse_unchecked checks magic
  // parse_unchecked 检查 magic
  assert!(Head::parse_unchecked(&record).is_none());
}

/// Test CRC mismatch
/// 测试 CRC 不匹配
#[test]
fn test_crc_mismatch() {
  let mut builder = HeadBuilder::new();
  let mut record = builder.infile(1, Flag::Infile, b"v", b"k").to_vec();
  // Corrupt CRC (at HEAD_TOTAL - 1, which is magic + HEAD_SIZE + CRC - 1)
  // 损坏 CRC
  record[HEAD_TOTAL - 1] ^= 0xFF;
  assert!(Head::parse(&record[1..], 0, 0).is_err());
}

/// Test head size
/// 测试头大小
#[test]
fn test_head_size() {
  let mut builder = HeadBuilder::new();

  // INFILE: record_size = HEAD_CRC + val + key
  let record = builder.infile(1, Flag::Infile, b"value", b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();
  assert_eq!(head.record_size(), HEAD_CRC + 5 + 3);

  // FILE: record_size = HEAD_CRC + key
  let record = builder.file(2, Flag::File, 100, 200, b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();
  assert_eq!(head.record_size(), HEAD_CRC + 3);

  // Tombstone: record_size = HEAD_CRC + key
  let record = builder.tombstone(3, b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();
  assert_eq!(head.record_size(), HEAD_CRC + 3);
}

/// Test LZ4 flag
/// 测试 LZ4 标志
#[test]
fn test_lz4_flag() {
  let mut builder = HeadBuilder::new();
  let record = builder.infile(1, Flag::InfileLz4, b"compressed", b"key");
  let head = Head::parse(&record[1..], 0, 0).unwrap();

  assert!(head.flag().is_lz4());
  assert!(head.val_is_infile());
}
