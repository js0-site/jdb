//! Pos module tests / Pos 模块测试

use std::{
  collections::hash_map::DefaultHasher,
  hash::{Hash, Hasher},
};

use jdb_val::Pos;

#[test]
fn test_new() {
  let pos = Pos::new(123, 456);
  assert_eq!(pos.id(), 123);
  assert_eq!(pos.pos(), 456);
}

#[test]
fn test_default() {
  let pos = Pos::default();
  assert_eq!(pos.id(), 0);
  assert_eq!(pos.pos(), 0);
}

#[test]
fn test_hash() {
  let p1 = Pos::new(1, 2);
  let p2 = Pos::new(1, 2);
  let p3 = Pos::new(1, 3);

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
  let p1 = Pos::new(1, 2);
  let p2 = Pos::new(1, 2);
  let p3 = Pos::new(2, 2);
  assert_eq!(p1, p2);
  assert_ne!(p1, p3);
}
