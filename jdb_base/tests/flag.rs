//! Flag tests
//! 标志测试

use jdb_base::Flag;

#[test]
fn test_flag_values() {
  assert_eq!(Flag::Infile as u8, 0);
  assert_eq!(Flag::InfileLz4 as u8, 1);
  assert_eq!(Flag::InfileZstd as u8, 2);
  assert_eq!(Flag::InfileProbed as u8, 3);
  assert_eq!(Flag::File as u8, 4);
  assert_eq!(Flag::FileLz4 as u8, 5);
  assert_eq!(Flag::FileZstd as u8, 6);
  assert_eq!(Flag::FileProbed as u8, 7);
}

#[test]
fn test_flag_from_u8() {
  assert_eq!(Flag::from_u8(0), Flag::Infile);
  assert_eq!(Flag::from_u8(1), Flag::InfileLz4);
  assert_eq!(Flag::from_u8(4), Flag::File);
  assert_eq!(Flag::from_u8(7), Flag::FileProbed);
  assert_eq!(Flag::from_u8(8), Flag::Tombstone);
  // Masked: 255 & 0xF = 15, > 8 => Tombstone
  assert_eq!(Flag::from_u8(255), Flag::Tombstone);
}

#[test]
fn test_flag_is_infile() {
  assert!(Flag::Infile.is_infile());
  assert!(Flag::InfileLz4.is_infile());
  assert!(Flag::InfileZstd.is_infile());
  assert!(Flag::InfileProbed.is_infile());
  assert!(!Flag::File.is_infile());
  assert!(!Flag::FileLz4.is_infile());
}

#[test]
fn test_flag_is_file() {
  assert!(!Flag::Infile.is_file());
  assert!(!Flag::InfileLz4.is_file());
  assert!(Flag::File.is_file());
  assert!(Flag::FileLz4.is_file());
  assert!(Flag::FileZstd.is_file());
  assert!(Flag::FileProbed.is_file());
}

#[test]
fn test_flag_is_compressed() {
  assert!(!Flag::Infile.is_compressed());
  assert!(Flag::InfileLz4.is_compressed());
  assert!(Flag::InfileZstd.is_compressed());
  assert!(!Flag::InfileProbed.is_compressed());
  assert!(!Flag::File.is_compressed());
  assert!(Flag::FileLz4.is_compressed());
  assert!(Flag::FileZstd.is_compressed());
  assert!(!Flag::FileProbed.is_compressed());
}

#[test]
fn test_flag_is_lz4() {
  assert!(!Flag::Infile.is_lz4());
  assert!(Flag::InfileLz4.is_lz4());
  assert!(!Flag::InfileZstd.is_lz4());
  assert!(Flag::FileLz4.is_lz4());
  assert!(!Flag::FileZstd.is_lz4());
}

#[test]
fn test_flag_is_zstd() {
  assert!(!Flag::Infile.is_zstd());
  assert!(!Flag::InfileLz4.is_zstd());
  assert!(Flag::InfileZstd.is_zstd());
  assert!(!Flag::FileLz4.is_zstd());
  assert!(Flag::FileZstd.is_zstd());
}

#[test]
fn test_flag_is_probed() {
  assert!(!Flag::Infile.is_probed());
  assert!(!Flag::InfileLz4.is_probed());
  assert!(Flag::InfileProbed.is_probed());
  assert!(!Flag::File.is_probed());
  assert!(Flag::FileProbed.is_probed());
}

#[test]
fn test_flag_to_lz4() {
  assert_eq!(Flag::Infile.to_lz4(), Flag::InfileLz4);
  assert_eq!(Flag::InfileProbed.to_lz4(), Flag::InfileLz4);
  assert_eq!(Flag::File.to_lz4(), Flag::FileLz4);
  assert_eq!(Flag::FileProbed.to_lz4(), Flag::FileLz4);
}

#[test]
fn test_flag_to_zstd() {
  assert_eq!(Flag::Infile.to_zstd(), Flag::InfileZstd);
  assert_eq!(Flag::File.to_zstd(), Flag::FileZstd);
}

#[test]
fn test_flag_to_probed() {
  assert_eq!(Flag::Infile.to_probed(), Flag::InfileProbed);
  assert_eq!(Flag::InfileLz4.to_probed(), Flag::InfileProbed);
  assert_eq!(Flag::File.to_probed(), Flag::FileProbed);
  assert_eq!(Flag::FileLz4.to_probed(), Flag::FileProbed);
}
