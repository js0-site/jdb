//! Head tests
//! 头测试

use jdb_val::{CRC_SIZE, FILE_ENTRY_SIZE, Flag, Head, HeadBuilder, MAGIC, Store};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Test infile key + infile val
/// 测试同文件 key + 同文件 val
#[test]
fn test_infile_infile() {
  let mut builder = HeadBuilder::new();
  let key = b"hello";
  let val = b"world";

  let bytes = builder.infile_infile(1, Store::Infile, key, Store::Infile, val);
  let head = Head::parse(bytes).unwrap();

  assert_eq!(head.id, 1);
  assert!(!head.is_tombstone());
  assert!(head.key_store().is_infile());
  assert!(head.val_store().is_infile());
  assert_eq!(head.key_len, 5);
  assert_eq!(head.val_len, Some(5));

  // Verify head_data
  // 验证 head_data
  let data_start = head.data_off;
  let data_end = data_start + head.head_len as usize;
  let head_data = &bytes[data_start..data_end];
  assert_eq!(head.key_data(head_data), key);
  assert_eq!(head.val_data(head_data), val);
}

/// Test infile key + file val
/// 测试同文件 key + 文件 val
#[test]
fn test_infile_file() {
  use jdb_val::FilePos;

  let mut builder = HeadBuilder::new();
  let key = b"mykey";
  let val_pos = FilePos::with_hash(100, 200, b"test_data");

  let bytes = builder.infile_file(2, Store::Infile, key, Store::File, &val_pos, 1024);
  let head = Head::parse(bytes).unwrap();

  assert_eq!(head.id, 2);
  assert!(head.key_store().is_infile());
  assert!(head.val_store().is_file());
  assert_eq!(head.key_len, 5);
  assert_eq!(head.val_len, Some(1024));

  let data_start = head.data_off;
  let data_end = data_start + head.head_len as usize;
  let head_data = &bytes[data_start..data_end];
  assert_eq!(head.key_data(head_data), key);

  let got_pos = head.val_file_pos(head_data);
  assert_eq!(got_pos.file_id, 100);
  assert_eq!(got_pos.offset, 200);
  assert_eq!(got_pos.hash, val_pos.hash);
}

/// Test file key + infile val
/// 测试文件 key + 同文件 val
#[test]
fn test_file_infile() {
  use jdb_val::FilePos;

  let mut builder = HeadBuilder::new();
  let key_pos = FilePos::with_hash(50, 60, b"key_data");
  let val = b"short_value";

  let bytes = builder.file_infile(3, Store::File, &key_pos, 128, Store::Infile, val);
  let head = Head::parse(bytes).unwrap();

  assert_eq!(head.id, 3);
  assert!(head.key_store().is_file());
  assert!(head.val_store().is_infile());
  assert_eq!(head.key_len, 128);
  assert_eq!(head.val_len, Some(val.len() as u64));

  let data_start = head.data_off;
  let data_end = data_start + head.head_len as usize;
  let head_data = &bytes[data_start..data_end];

  let got_pos = head.key_file_pos(head_data);
  assert_eq!(got_pos.file_id, 50);
  assert_eq!(got_pos.offset, 60);
  assert_eq!(head.val_data(head_data), val);
}

/// Test file key + file val
/// 测试文件 key + 文件 val
#[test]
fn test_file_file() {
  use jdb_val::FilePos;

  let mut builder = HeadBuilder::new();
  let key_pos = FilePos::with_hash(10, 20, b"key");
  let val_pos = FilePos::with_hash(30, 40, b"val");

  let bytes = builder.file_file(4, Store::File, &key_pos, 256, Store::File, &val_pos, 4096);
  let head = Head::parse(bytes).unwrap();

  assert_eq!(head.id, 4);
  assert!(head.key_store().is_file());
  assert!(head.val_store().is_file());
  assert_eq!(head.key_len, 256);
  assert_eq!(head.val_len, Some(4096));

  let data_start = head.data_off;
  let data_end = data_start + head.head_len as usize;
  let head_data = &bytes[data_start..data_end];

  let k_pos = head.key_file_pos(head_data);
  assert_eq!(k_pos.file_id, 10);
  assert_eq!(k_pos.offset, 20);

  let v_pos = head.val_file_pos(head_data);
  assert_eq!(v_pos.file_id, 30);
  assert_eq!(v_pos.offset, 40);
}

/// Test tombstone infile
/// 测试同文件删除标记
#[test]
fn test_tombstone_infile() {
  let mut builder = HeadBuilder::new();
  let key = b"deleted_key";

  let bytes = builder.tombstone_infile(5, Store::Infile, key);
  let head = Head::parse(bytes).unwrap();

  assert_eq!(head.id, 5);
  assert!(head.is_tombstone());
  assert!(head.key_store().is_infile());
  assert_eq!(head.key_len, key.len() as u64);
  assert_eq!(head.val_len, None);

  let data_start = head.data_off;
  let data_end = data_start + head.head_len as usize;
  let head_data = &bytes[data_start..data_end];
  assert_eq!(head.key_data(head_data), key);
}

/// Test tombstone file
/// 测试文件删除标记
#[test]
fn test_tombstone_file() {
  use jdb_val::FilePos;

  let mut builder = HeadBuilder::new();
  let key_pos = FilePos::with_hash(99, 88, b"big_key");

  let bytes = builder.tombstone_file(6, Store::File, &key_pos, 1024);
  let head = Head::parse(bytes).unwrap();

  assert_eq!(head.id, 6);
  assert!(head.is_tombstone());
  assert!(head.key_store().is_file());
  assert_eq!(head.key_len, 1024);
  assert_eq!(head.val_len, None);
}

/// Test CRC verification
/// 测试 CRC 校验
#[test]
fn test_crc_verify() {
  let mut builder = HeadBuilder::new();
  let bytes = builder.infile_infile(1, Store::Infile, b"k", Store::Infile, b"v");

  // CRC is calculated from id (skip magic)
  // CRC 从 id 开始计算（跳过 magic）
  let crc_off = bytes.len() - CRC_SIZE;
  let stored = u32::from_le_bytes(bytes[crc_off..].try_into().unwrap());
  let computed = crc32fast::hash(&bytes[1..crc_off]);
  assert_eq!(stored, computed);
}

/// Test magic byte
/// 测试魔数字节
#[test]
fn test_magic() {
  let mut builder = HeadBuilder::new();
  let bytes = builder.infile_infile(1, Store::Infile, b"k", Store::Infile, b"v");
  assert_eq!(bytes[0], MAGIC);
}

/// Test invalid magic
/// 测试无效魔数
#[test]
fn test_invalid_magic() {
  let mut builder = HeadBuilder::new();
  let mut bytes = builder
    .infile_infile(1, Store::Infile, b"k", Store::Infile, b"v")
    .to_vec();
  bytes[0] = 0x00;
  assert!(Head::parse(&bytes).is_err());
}

/// Test CRC mismatch
/// 测试 CRC 不匹配
#[test]
fn test_crc_mismatch() {
  let mut builder = HeadBuilder::new();
  let mut bytes = builder
    .infile_infile(1, Store::Infile, b"k", Store::Infile, b"v")
    .to_vec();
  // Corrupt CRC
  // 损坏 CRC
  let crc_off = bytes.len() - CRC_SIZE;
  bytes[crc_off] ^= 0xFF;
  assert!(Head::parse(&bytes).is_err());
}

/// Test head size calculation
/// 测试头大小计算
#[test]
fn test_head_size() {
  let mut builder = HeadBuilder::new();

  // Small infile
  // 小同文件
  let bytes = builder.infile_infile(1, Store::Infile, b"k", Store::Infile, b"v");
  let head = Head::parse(bytes).unwrap();
  assert_eq!(head.size, bytes.len());

  // File mode (32B per entry)
  // 文件模式（每条目 32 字节）
  use jdb_val::FilePos;
  let pos = FilePos::with_hash(1, 2, b"data");
  let bytes = builder.file_file(1, Store::File, &pos, 100, Store::File, &pos, 200);
  let head = Head::parse(bytes).unwrap();
  assert_eq!(head.head_len as usize, FILE_ENTRY_SIZE * 2);
}

/// Test Flag encoding
/// 测试 Flag 编码
#[test]
fn test_flag_encoding() {
  let flag = Flag::new(Store::Infile, Store::File);
  assert_eq!(flag.key(), Store::Infile);
  assert_eq!(flag.val(), Store::File);

  let flag = Flag::new(Store::FileLz4, Store::InfileZstd);
  assert_eq!(flag.key(), Store::FileLz4);
  assert_eq!(flag.val(), Store::InfileZstd);
}
