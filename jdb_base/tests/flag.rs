//! Flag tests
//! 标志测试

use jdb_base::Flag;

#[test]
fn test_flag_values() {
  assert_eq!(Flag::INFILE.as_u8(), 0);
  assert_eq!(Flag::INFILE_LZ4.as_u8(), 1);
  assert_eq!(Flag::INFILE_ZSTD.as_u8(), 2);
  assert_eq!(Flag::INFILE_PROBED.as_u8(), 3);
  assert_eq!(Flag::FILE.as_u8(), 4);
  assert_eq!(Flag::FILE_LZ4.as_u8(), 5);
  assert_eq!(Flag::FILE_ZSTD.as_u8(), 6);
  assert_eq!(Flag::FILE_PROBED.as_u8(), 7);
}

#[test]
fn test_flag_from_u8() {
  assert_eq!(Flag::from_u8(0), Flag::INFILE);
  assert_eq!(Flag::from_u8(1), Flag::INFILE_LZ4);
  assert_eq!(Flag::from_u8(4), Flag::FILE);
  assert_eq!(Flag::from_u8(7), Flag::FILE_PROBED);
  // 8+ has tombstone bit set
  // 8+ 有墓碑位
  assert!(Flag::from_u8(8).is_tombstone());
  assert!(Flag::from_u8(15).is_tombstone());
}

#[test]
fn test_flag_is_infile() {
  assert!(Flag::INFILE.is_infile());
  assert!(Flag::INFILE_LZ4.is_infile());
  assert!(Flag::INFILE_ZSTD.is_infile());
  assert!(Flag::INFILE_PROBED.is_infile());
  assert!(!Flag::FILE.is_infile());
  assert!(!Flag::FILE_LZ4.is_infile());
}

#[test]
fn test_flag_is_file() {
  assert!(!Flag::INFILE.is_file());
  assert!(!Flag::INFILE_LZ4.is_file());
  assert!(Flag::FILE.is_file());
  assert!(Flag::FILE_LZ4.is_file());
  assert!(Flag::FILE_ZSTD.is_file());
  assert!(Flag::FILE_PROBED.is_file());
}

#[test]
fn test_flag_is_compressed() {
  assert!(!Flag::INFILE.is_compressed());
  assert!(Flag::INFILE_LZ4.is_compressed());
  assert!(Flag::INFILE_ZSTD.is_compressed());
  assert!(!Flag::INFILE_PROBED.is_compressed());
  assert!(!Flag::FILE.is_compressed());
  assert!(Flag::FILE_LZ4.is_compressed());
  assert!(Flag::FILE_ZSTD.is_compressed());
  assert!(!Flag::FILE_PROBED.is_compressed());
}

#[test]
fn test_flag_is_lz4() {
  assert!(!Flag::INFILE.is_lz4());
  assert!(Flag::INFILE_LZ4.is_lz4());
  assert!(!Flag::INFILE_ZSTD.is_lz4());
  assert!(Flag::FILE_LZ4.is_lz4());
  assert!(!Flag::FILE_ZSTD.is_lz4());
}

#[test]
fn test_flag_is_zstd() {
  assert!(!Flag::INFILE.is_zstd());
  assert!(!Flag::INFILE_LZ4.is_zstd());
  assert!(Flag::INFILE_ZSTD.is_zstd());
  assert!(!Flag::FILE_LZ4.is_zstd());
  assert!(Flag::FILE_ZSTD.is_zstd());
}

#[test]
fn test_flag_is_probed() {
  assert!(!Flag::INFILE.is_probed());
  assert!(!Flag::INFILE_LZ4.is_probed());
  assert!(Flag::INFILE_PROBED.is_probed());
  assert!(!Flag::FILE.is_probed());
  assert!(Flag::FILE_PROBED.is_probed());
}

#[test]
fn test_flag_to_lz4() {
  assert_eq!(Flag::INFILE.to_lz4(), Flag::INFILE_LZ4);
  assert_eq!(Flag::INFILE_PROBED.to_lz4(), Flag::INFILE_LZ4);
  assert_eq!(Flag::FILE.to_lz4(), Flag::FILE_LZ4);
  assert_eq!(Flag::FILE_PROBED.to_lz4(), Flag::FILE_LZ4);
}

#[test]
fn test_flag_to_zstd() {
  assert_eq!(Flag::INFILE.to_zstd(), Flag::INFILE_ZSTD);
  assert_eq!(Flag::FILE.to_zstd(), Flag::FILE_ZSTD);
}

#[test]
fn test_flag_to_probed() {
  assert_eq!(Flag::INFILE.to_probed(), Flag::INFILE_PROBED);
  assert_eq!(Flag::INFILE_LZ4.to_probed(), Flag::INFILE_PROBED);
  assert_eq!(Flag::FILE.to_probed(), Flag::FILE_PROBED);
  assert_eq!(Flag::FILE_LZ4.to_probed(), Flag::FILE_PROBED);
}

#[test]
fn test_tombstone() {
  // Tombstone preserves storage type
  // 墓碑保留存储类型
  let infile_tomb = Flag::INFILE.to_tombstone();
  assert!(infile_tomb.is_tombstone());
  assert!(infile_tomb.is_infile());
  assert_eq!(infile_tomb.storage(), Flag::INFILE);

  let file_tomb = Flag::FILE_LZ4.to_tombstone();
  assert!(file_tomb.is_tombstone());
  assert!(file_tomb.is_file());
  assert!(file_tomb.is_lz4());
  assert_eq!(file_tomb.storage(), Flag::FILE_LZ4);
}
