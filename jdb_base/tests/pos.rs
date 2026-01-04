//! Pos module tests / Pos 模块测试

use std::{
  collections::hash_map::DefaultHasher,
  hash::{Hash, Hasher},
};

use jdb_base::{Flag, Pos};

#[test]
fn test_infile() {
  let pos = Pos::infile(123, 456, 100);
  assert_eq!(pos.id(), 123);
  assert_eq!(pos.offset(), 456);
  assert_eq!(pos.len(), 100);
  assert!(pos.is_infile());
  assert_eq!(pos.flag(), Flag::Infile);
}

#[test]
fn test_infile_with_flag() {
  let pos = Pos::infile_with_flag(Flag::InfileLz4, 123, 456, 100);
  assert_eq!(pos.id(), 123);
  assert_eq!(pos.offset(), 456);
  assert_eq!(pos.len(), 100);
  assert!(pos.is_infile());
  assert_eq!(pos.flag(), Flag::InfileLz4);
}

#[test]
fn test_file() {
  let pos = Pos::file(123, 789, 200);
  assert_eq!(pos.id(), 123);
  assert_eq!(pos.file_id(), 789);
  assert_eq!(pos.len(), 200);
  assert!(!pos.is_infile());
  assert_eq!(pos.flag(), Flag::File);
}

#[test]
fn test_file_with_flag() {
  let pos = Pos::file_with_flag(Flag::FileLz4, 123, 789, 200);
  assert_eq!(pos.id(), 123);
  assert_eq!(pos.file_id(), 789);
  assert_eq!(pos.len(), 200);
  assert!(!pos.is_infile());
  assert_eq!(pos.flag(), Flag::FileLz4);
}

#[test]
fn test_tombstone() {
  let pos = Pos::tombstone(123, 456);
  assert_eq!(pos.id(), 123);
  assert_eq!(pos.offset(), 456);
  assert_eq!(pos.len(), 0);
  assert!(pos.is_tombstone());
  assert!(pos.is_empty());
  assert_eq!(pos.flag(), Flag::Tombstone);
}

#[test]
fn test_default() {
  let pos = Pos::default();
  assert_eq!(pos.id(), 0);
  assert_eq!(pos.offset(), 0);
  assert_eq!(pos.len(), 0);
  assert!(pos.is_empty());
  assert_eq!(pos.flag(), Flag::Infile);
}

#[test]
fn test_hash() {
  let p1 = Pos::infile(1, 2, 10);
  let p2 = Pos::infile(1, 2, 10);
  let p3 = Pos::infile(1, 3, 10);

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
  let p1 = Pos::infile(1, 2, 10);
  let p2 = Pos::infile(1, 2, 10);
  let p3 = Pos::infile(2, 2, 10);
  assert_eq!(p1, p2);
  assert_ne!(p1, p3);
}

#[test]
fn test_size() {
  assert_eq!(Pos::SIZE, 24);
  assert_eq!(std::mem::size_of::<Pos>(), 24);
}
