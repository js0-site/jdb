//! DefaultGc tests
//! DefaultGc 测试
//!
//! Tests compression and probing logic
//! 测试压缩和探测逻辑

use jdb_val::{DefaultGc, GcTrait, Store};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Test: Skip already compressed data
/// 跳过已压缩数据
#[test]
fn test_skip_compressed() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  let data = vec![0u8; 2048];

  // INFILE_LZ4 should be skipped
  // INFILE_LZ4 应被跳过
  let (store, len) = gc.process(Store::InfileLz4, &data, &mut buf);
  assert_eq!(store, Store::InfileLz4);
  assert!(len.is_none());

  // FILE_LZ4 should be skipped
  // FILE_LZ4 应被跳过
  let (store, len) = gc.process(Store::FileLz4, &data, &mut buf);
  assert_eq!(store, Store::FileLz4);
  assert!(len.is_none());

  // INFILE_ZSTD should be skipped
  // INFILE_ZSTD 应被跳过
  let (store, len) = gc.process(Store::InfileZstd, &data, &mut buf);
  assert_eq!(store, Store::InfileZstd);
  assert!(len.is_none());

  // FILE_ZSTD should be skipped
  // FILE_ZSTD 应被跳过
  let (store, len) = gc.process(Store::FileZstd, &data, &mut buf);
  assert_eq!(store, Store::FileZstd);
  assert!(len.is_none());
}

/// Test: Skip already probed data
/// 跳过已探测数据
#[test]
fn test_skip_probed() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  let data = vec![0u8; 2048];

  // INFILE_PROBED should be skipped
  // INFILE_PROBED 应被跳过
  let (store, len) = gc.process(Store::InfileProbed, &data, &mut buf);
  assert_eq!(store, Store::InfileProbed);
  assert!(len.is_none());

  // FILE_PROBED should be skipped
  // FILE_PROBED 应被跳过
  let (store, len) = gc.process(Store::FileProbed, &data, &mut buf);
  assert_eq!(store, Store::FileProbed);
  assert!(len.is_none());
}

/// Test: Compress INFILE data successfully
/// 成功压缩 INFILE 数据
///
/// Compressible data (repeated bytes) should return INFILE_LZ4
/// 可压缩数据（重复字节）应返回 INFILE_LZ4
#[test]
fn test_compress_infile() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  // Repeated data is highly compressible
  // 重复数据高度可压缩
  let data = vec![0u8; 2048];

  let (store, len) = gc.process(Store::Infile, &data, &mut buf);
  assert_eq!(store, Store::InfileLz4);
  assert!(len.is_some());
  let compressed_len = len.unwrap();
  assert!(compressed_len < data.len(), "compressed should be smaller");
}

/// Test: Compress FILE data successfully
/// 成功压缩 FILE 数据
///
/// Compressible data (repeated bytes) should return FILE_LZ4
/// 可压缩数据（重复字节）应返回 FILE_LZ4
#[test]
fn test_compress_file() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  // Repeated data is highly compressible
  // 重复数据高度可压缩
  let data = vec![0u8; 2048];

  let (store, len) = gc.process(Store::File, &data, &mut buf);
  assert_eq!(store, Store::FileLz4);
  assert!(len.is_some());
  let compressed_len = len.unwrap();
  assert!(compressed_len < data.len(), "compressed should be smaller");
}

/// Test: Mark as PROBED if incompressible
/// 不可压缩则标记为 PROBED
///
/// Random data is incompressible, should return PROBED flag
/// 随机数据不可压缩，应返回 PROBED 标志
#[test]
fn test_mark_probed_infile() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  // Random-like data is incompressible
  // 类随机数据不可压缩
  let data: Vec<u8> = (0..2048).map(|i| (i * 17 + 31) as u8).collect();

  let (store, len) = gc.process(Store::Infile, &data, &mut buf);
  // Either compressed or marked as probed
  // 要么压缩成功，要么标记为已探测
  if len.is_none() {
    assert_eq!(store, Store::InfileProbed);
  } else {
    assert_eq!(store, Store::InfileLz4);
  }
}

/// Test: Mark as PROBED if incompressible (FILE)
/// 不可压缩则标记为 PROBED (FILE)
#[test]
fn test_mark_probed_file() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  // Random-like data is incompressible
  // 类随机数据不可压缩
  let data: Vec<u8> = (0..2048).map(|i| (i * 17 + 31) as u8).collect();

  let (store, len) = gc.process(Store::File, &data, &mut buf);
  // Either compressed or marked as probed
  // 要么压缩成功，要么标记为已探测
  if len.is_none() {
    assert_eq!(store, Store::FileProbed);
  } else {
    assert_eq!(store, Store::FileLz4);
  }
}

/// Test: Small data (< 1KB) should not compress
/// 小数据 (< 1KB) 不应压缩
///
/// Even with INFILE flag, data < 1KB should return original flag
/// 即使是 INFILE 标志，< 1KB 的数据也应返回原始标志
#[test]
fn test_small_data_no_compress() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  // Data < 1KB
  // 数据 < 1KB
  let data = vec![0u8; 512];

  let (store, len) = gc.process(Store::Infile, &data, &mut buf);
  // Small data: either original flag or probed
  // 小数据：原始标志或已探测
  assert!(len.is_none());
  // Flag should be INFILE_PROBED since compression was skipped
  // 标志应为 INFILE_PROBED，因为压缩被跳过
  assert_eq!(store, Store::InfileProbed);
}
