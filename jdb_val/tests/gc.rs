//! DefaultGc tests
//! DefaultGc 测试
//!
//! Tests GC trait implementations (no compression, upstream handles it)
//! 测试 GC trait 实现（不压缩，上游处理）

use jdb_base::Flag;
use jdb_val::{DefaultGc, GcTrait, NoGc};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Test: DefaultGc passes through all flags unchanged
/// DefaultGc 透传所有标志不变
#[test]
fn test_default_gc_passthrough() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  let data = vec![0u8; 2048];

  // All flags should pass through unchanged
  // 所有标志应透传不变
  let flags = [
    Flag::Infile,
    Flag::File,
    Flag::InfileLz4,
    Flag::FileLz4,
    Flag::InfileZstd,
    Flag::FileZstd,
    Flag::InfileProbed,
    Flag::FileProbed,
    Flag::Tombstone,
  ];

  for flag in flags {
    let (result_flag, len) = gc.process(flag, &data, &mut buf);
    assert_eq!(result_flag, flag, "flag should pass through unchanged");
    assert!(len.is_none(), "no compression should occur");
  }
}

/// Test: NoGc passes through all flags unchanged
/// NoGc 透传所有标志不变
#[test]
fn test_no_gc_passthrough() {
  let mut gc = NoGc;
  let mut buf = Vec::new();
  let data = vec![0u8; 2048];

  // All flags should pass through unchanged
  // 所有标志应透传不变
  let flags = [
    Flag::Infile,
    Flag::File,
    Flag::InfileLz4,
    Flag::FileLz4,
    Flag::InfileZstd,
    Flag::FileZstd,
    Flag::InfileProbed,
    Flag::FileProbed,
    Flag::Tombstone,
  ];

  for flag in flags {
    let (result_flag, len) = gc.process(flag, &data, &mut buf);
    assert_eq!(result_flag, flag, "flag should pass through unchanged");
    assert!(len.is_none(), "no compression should occur");
  }
}

/// Test: DefaultGc with empty data
/// DefaultGc 处理空数据
#[test]
fn test_default_gc_empty_data() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  let data: Vec<u8> = vec![];

  let (flag, len) = gc.process(Flag::Infile, &data, &mut buf);
  assert_eq!(flag, Flag::Infile);
  assert!(len.is_none());
}

/// Test: DefaultGc with small data
/// DefaultGc 处理小数据
#[test]
fn test_default_gc_small_data() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  let data = vec![0u8; 512];

  let (flag, len) = gc.process(Flag::Infile, &data, &mut buf);
  assert_eq!(flag, Flag::Infile);
  assert!(len.is_none());
}

/// Test: Buffer is not modified
/// 缓冲区不被修改
#[test]
fn test_buffer_not_modified() {
  let mut gc = DefaultGc;
  let mut buf = Vec::new();
  let data = vec![0u8; 2048];

  gc.process(Flag::Infile, &data, &mut buf);
  assert!(buf.is_empty(), "buffer should not be modified");
}
