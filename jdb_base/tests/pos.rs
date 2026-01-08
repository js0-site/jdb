//! Pos module tests / Pos 模块测试

use std::{
  collections::hash_map::DefaultHasher,
  hash::{Hash, Hasher},
};

use jdb_base::{Flag, Pos};

#[test]
fn test_infile() {
  let pos = Pos::new(1, Flag::INFILE, 123, 456, 100);
  assert_eq!(pos.ver(), 1);
  assert_eq!(pos.wal_id(), 123);
  assert_eq!(pos.offset(), 456);
  assert_eq!(pos.len(), 100);
  assert!(pos.is_infile());
  assert_eq!(pos.flag(), Flag::INFILE);
}

#[test]
fn test_infile_lz4() {
  let pos = Pos::new(1, Flag::INFILE_LZ4, 123, 456, 100);
  assert!(pos.is_infile());
  assert_eq!(pos.flag(), Flag::INFILE_LZ4);
}

#[test]
fn test_file() {
  let pos = Pos::new(1, Flag::FILE, 123, 789, 200);
  assert_eq!(pos.ver(), 1);
  assert_eq!(pos.wal_id(), 123);
  assert_eq!(pos.file_id(), 789);
  assert_eq!(pos.len(), 200);
  assert!(pos.is_file());
  assert_eq!(pos.flag(), Flag::FILE);
}

#[test]
fn test_file_lz4() {
  let pos = Pos::new(1, Flag::FILE_LZ4, 123, 789, 200);
  assert!(pos.is_file());
  assert_eq!(pos.flag(), Flag::FILE_LZ4);
}

#[test]
fn test_tombstone() {
  let pos = Pos::new(1, Flag::INFILE, 123, 456, 100);
  let tomb = pos.to_tombstone();
  assert_eq!(tomb.ver(), 1);
  assert_eq!(tomb.wal_id(), 123);
  assert_eq!(tomb.offset(), 456);
  assert_eq!(tomb.len(), 100);
  assert!(tomb.is_tombstone());
  assert!(tomb.is_infile());
  assert_eq!(tomb.storage(), Flag::INFILE);
}

#[test]
fn test_file_tombstone() {
  let pos = Pos::new(1, Flag::FILE_LZ4, 123, 789, 200);
  let tomb = pos.to_tombstone();
  assert!(tomb.is_tombstone());
  assert!(tomb.is_file());
  assert_eq!(tomb.file_id(), 789);
  assert_eq!(tomb.len(), 200);
  assert_eq!(tomb.storage(), Flag::FILE_LZ4);
}

#[test]
fn test_default() {
  let pos = Pos::default();
  assert_eq!(pos.wal_id(), 0);
  assert_eq!(pos.offset(), 0);
  assert_eq!(pos.len(), 0);
  assert!(pos.is_empty());
  assert_eq!(pos.flag(), Flag::INFILE);
}

#[test]
fn test_hash() {
  let p1 = Pos::new(1, Flag::INFILE, 1, 2, 10);
  let p2 = Pos::new(1, Flag::INFILE, 1, 2, 10);
  let p3 = Pos::new(1, Flag::INFILE, 1, 3, 10);

  let mut h1 = DefaultHasher::new();
  let mut h2 = DefaultHasher::new();
  let mut h3 = DefaultHasher::new();

  p1.hash(&mut h1);
  p2.hash(&mut h2);
  p3.hash(&mut h3);

  assert_eq!(h1.finish(), h2.finish());
  assert_ne!(h1.finish(), h3.finish());
}

#[test]
fn test_eq() {
  let p1 = Pos::new(1, Flag::INFILE, 1, 2, 10);
  let p2 = Pos::new(1, Flag::INFILE, 1, 2, 10);
  let p3 = Pos::new(2, Flag::INFILE, 2, 2, 10);
  assert_eq!(p1, p2);
  assert_ne!(p1, p3);
}

#[test]
fn test_size() {
  assert_eq!(Pos::SIZE, 32);
  assert_eq!(std::mem::size_of::<Pos>(), 32);
}
