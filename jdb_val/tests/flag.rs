//! Flag tests
//! 标志测试

use jdb_val::{Flag, Store};

#[test]
fn test_store_values() {
  assert_eq!(Store::Infile as u8, 0);
  assert_eq!(Store::InfileLz4 as u8, 1);
  assert_eq!(Store::InfileZstd as u8, 2);
  assert_eq!(Store::InfileProbed as u8, 3);
  assert_eq!(Store::File as u8, 4);
  assert_eq!(Store::FileLz4 as u8, 5);
  assert_eq!(Store::FileZstd as u8, 6);
  assert_eq!(Store::FileProbed as u8, 7);
}

#[test]
fn test_store_from_u8() {
  assert_eq!(Store::from_u8(0), Store::Infile);
  assert_eq!(Store::from_u8(1), Store::InfileLz4);
  assert_eq!(Store::from_u8(2), Store::InfileZstd);
  assert_eq!(Store::from_u8(3), Store::InfileProbed);
  assert_eq!(Store::from_u8(4), Store::File);
  assert_eq!(Store::from_u8(5), Store::FileLz4);
  assert_eq!(Store::from_u8(6), Store::FileZstd);
  assert_eq!(Store::from_u8(7), Store::FileProbed);
  // Overflow wraps
  // 溢出回绕
  assert_eq!(Store::from_u8(8), Store::Infile);
}

#[test]
fn test_store_is_infile() {
  assert!(Store::Infile.is_infile());
  assert!(Store::InfileLz4.is_infile());
  assert!(Store::InfileZstd.is_infile());
  assert!(Store::InfileProbed.is_infile());
  assert!(!Store::File.is_infile());
  assert!(!Store::FileLz4.is_infile());
  assert!(!Store::FileZstd.is_infile());
  assert!(!Store::FileProbed.is_infile());
}

#[test]
fn test_store_is_file() {
  assert!(!Store::Infile.is_file());
  assert!(!Store::InfileLz4.is_file());
  assert!(!Store::InfileZstd.is_file());
  assert!(!Store::InfileProbed.is_file());
  assert!(Store::File.is_file());
  assert!(Store::FileLz4.is_file());
  assert!(Store::FileZstd.is_file());
  assert!(Store::FileProbed.is_file());
}

#[test]
fn test_store_is_compressed() {
  assert!(!Store::Infile.is_compressed());
  assert!(Store::InfileLz4.is_compressed());
  assert!(Store::InfileZstd.is_compressed());
  assert!(!Store::InfileProbed.is_compressed());
  assert!(!Store::File.is_compressed());
  assert!(Store::FileLz4.is_compressed());
  assert!(Store::FileZstd.is_compressed());
  assert!(!Store::FileProbed.is_compressed());
}

#[test]
fn test_store_is_lz4() {
  assert!(!Store::Infile.is_lz4());
  assert!(Store::InfileLz4.is_lz4());
  assert!(!Store::InfileZstd.is_lz4());
  assert!(!Store::File.is_lz4());
  assert!(Store::FileLz4.is_lz4());
  assert!(!Store::FileZstd.is_lz4());
}

#[test]
fn test_store_is_zstd() {
  assert!(!Store::Infile.is_zstd());
  assert!(!Store::InfileLz4.is_zstd());
  assert!(Store::InfileZstd.is_zstd());
  assert!(!Store::File.is_zstd());
  assert!(!Store::FileLz4.is_zstd());
  assert!(Store::FileZstd.is_zstd());
}

#[test]
fn test_store_is_probed() {
  assert!(!Store::Infile.is_probed());
  assert!(!Store::InfileLz4.is_probed());
  assert!(!Store::InfileZstd.is_probed());
  assert!(Store::InfileProbed.is_probed());
  assert!(!Store::File.is_probed());
  assert!(!Store::FileLz4.is_probed());
  assert!(!Store::FileZstd.is_probed());
  assert!(Store::FileProbed.is_probed());
}

#[test]
fn test_store_to_probed() {
  assert_eq!(Store::Infile.to_probed(), Store::InfileProbed);
  assert_eq!(Store::InfileLz4.to_probed(), Store::InfileProbed);
  assert_eq!(Store::InfileZstd.to_probed(), Store::InfileProbed);
  assert_eq!(Store::InfileProbed.to_probed(), Store::InfileProbed);
  assert_eq!(Store::File.to_probed(), Store::FileProbed);
  assert_eq!(Store::FileLz4.to_probed(), Store::FileProbed);
  assert_eq!(Store::FileZstd.to_probed(), Store::FileProbed);
  assert_eq!(Store::FileProbed.to_probed(), Store::FileProbed);
}

#[test]
fn test_store_to_lz4() {
  assert_eq!(Store::Infile.to_lz4(), Store::InfileLz4);
  assert_eq!(Store::InfileProbed.to_lz4(), Store::InfileLz4);
  assert_eq!(Store::File.to_lz4(), Store::FileLz4);
  assert_eq!(Store::FileProbed.to_lz4(), Store::FileLz4);
}

#[test]
fn test_store_to_zstd() {
  assert_eq!(Store::Infile.to_zstd(), Store::InfileZstd);
  assert_eq!(Store::InfileProbed.to_zstd(), Store::InfileZstd);
  assert_eq!(Store::File.to_zstd(), Store::FileZstd);
  assert_eq!(Store::FileProbed.to_zstd(), Store::FileZstd);
}

#[test]
fn test_flag_new() {
  let flag = Flag::new(Store::Infile, Store::File);
  assert_eq!(flag.key(), Store::Infile);
  assert_eq!(flag.val(), Store::File);
}

#[test]
fn test_flag_encoding() {
  // key=0 (Infile), val=4 (File) => 0 | (4 << 3) = 32
  let flag = Flag::new(Store::Infile, Store::File);
  assert_eq!(flag.as_u8(), 32);

  // key=5 (FileLz4), val=2 (InfileZstd) => 5 | (2 << 3) = 21
  let flag = Flag::new(Store::FileLz4, Store::InfileZstd);
  assert_eq!(flag.as_u8(), 21);
}

#[test]
fn test_flag_from_u8() {
  let flag = Flag::from_u8(32);
  assert_eq!(flag.key(), Store::Infile);
  assert_eq!(flag.val(), Store::File);

  let flag = Flag::from_u8(21);
  assert_eq!(flag.key(), Store::FileLz4);
  assert_eq!(flag.val(), Store::InfileZstd);
}

#[test]
fn test_flag_roundtrip() {
  for k in 0..8u8 {
    for v in 0..8u8 {
      let key = Store::from_u8(k);
      let val = Store::from_u8(v);
      let flag = Flag::new(key, val);
      assert_eq!(flag.key(), key);
      assert_eq!(flag.val(), val);

      let flag2 = Flag::from_u8(flag.as_u8());
      assert_eq!(flag2.key(), key);
      assert_eq!(flag2.val(), val);
    }
  }
}
