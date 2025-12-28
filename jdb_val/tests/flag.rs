//! Flag module tests / Flag 模块测试

use jdb_val::Flag;

#[test]
fn test_inline() {
  assert!(Flag::INLINE.is_inline());
  assert!(!Flag::INLINE.is_infile());
  assert!(!Flag::INLINE.is_file());
  assert!(!Flag::INLINE.is_compressed());
}

#[test]
fn test_infile() {
  assert!(!Flag::INFILE.is_inline());
  assert!(Flag::INFILE.is_infile());
  assert!(!Flag::INFILE.is_file());
  assert!(!Flag::INFILE.is_compressed());

  assert!(Flag::INFILE_LZ4.is_infile());
  assert!(Flag::INFILE_LZ4.is_compressed());
  assert!(Flag::INFILE_LZ4.is_lz4());

  assert!(Flag::INFILE_ZSTD.is_infile());
  assert!(Flag::INFILE_ZSTD.is_compressed());
  assert!(Flag::INFILE_ZSTD.is_zstd());
}

#[test]
fn test_file() {
  assert!(!Flag::FILE.is_inline());
  assert!(!Flag::FILE.is_infile());
  assert!(Flag::FILE.is_file());
  assert!(!Flag::FILE.is_compressed());

  assert!(Flag::FILE_LZ4.is_file());
  assert!(Flag::FILE_LZ4.is_lz4());

  assert!(Flag::FILE_ZSTD.is_file());
  assert!(Flag::FILE_ZSTD.is_zstd());
}

#[test]
fn test_from() {
  assert_eq!(Flag::from(0x00), Flag::INLINE);
  assert_eq!(u8::from(Flag::FILE), 0x04);
}

#[test]
fn test_tombstone() {
  assert!(Flag::TOMBSTONE.is_tombstone());
  assert!(!Flag::INLINE.is_tombstone());
  assert!(!Flag::FILE.is_tombstone());
}
