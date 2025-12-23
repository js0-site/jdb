use aok::{OK, Void};
use jdb_tag::TagIndex;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_tag_add_get() -> Void {
  let mut idx = TagIndex::new();

  idx.add(1, b"region", b"us");
  idx.add(2, b"region", b"us");
  idx.add(3, b"region", b"eu");

  let us = idx.get(b"region", b"us").unwrap();
  assert!(us.contains(1));
  assert!(us.contains(2));
  assert!(!us.contains(3));

  let eu = idx.get(b"region", b"eu").unwrap();
  assert!(eu.contains(3));

  info!("tag add/get ok");
  OK
}

#[test]
fn test_tag_remove() -> Void {
  let mut idx = TagIndex::new();

  idx.add(1, b"type", b"sensor");
  idx.add(2, b"type", b"sensor");

  idx.remove(1, b"type", b"sensor");

  let sensors = idx.get(b"type", b"sensor").unwrap();
  assert!(!sensors.contains(1));
  assert!(sensors.contains(2));

  info!("tag remove ok");
  OK
}

#[test]
fn test_tag_and() -> Void {
  let mut idx = TagIndex::new();

  // Device 1: region=us, type=sensor
  idx.add(1, b"region", b"us");
  idx.add(1, b"type", b"sensor");

  // Device 2: region=us, type=gateway
  idx.add(2, b"region", b"us");
  idx.add(2, b"type", b"gateway");

  // Device 3: region=eu, type=sensor
  idx.add(3, b"region", b"eu");
  idx.add(3, b"type", b"sensor");

  // AND: region=us AND type=sensor
  let result = idx.and(&[(b"region".as_slice(), b"us".as_slice()), (b"type", b"sensor")]);
  assert_eq!(result.len(), 1);
  assert!(result.contains(1));

  info!("tag and ok");
  OK
}

#[test]
fn test_tag_or() -> Void {
  let mut idx = TagIndex::new();

  idx.add(1, b"region", b"us");
  idx.add(2, b"region", b"eu");
  idx.add(3, b"region", b"asia");

  // OR: region=us OR region=eu
  let result = idx.or(&[(b"region".as_slice(), b"us".as_slice()), (b"region", b"eu")]);
  assert_eq!(result.len(), 2);
  assert!(result.contains(1));
  assert!(result.contains(2));
  assert!(!result.contains(3));

  info!("tag or ok");
  OK
}

#[test]
fn test_tag_not() -> Void {
  let mut idx = TagIndex::new();

  idx.add(1, b"status", b"active");
  idx.add(2, b"status", b"active");
  idx.add(3, b"status", b"inactive");

  // Get all active, then exclude id 1
  let active = idx.get(b"status", b"active").unwrap().clone();
  let result = idx.not(&active, b"status", b"inactive");

  assert_eq!(result.len(), 2);
  assert!(result.contains(1));
  assert!(result.contains(2));

  info!("tag not ok");
  OK
}

#[test]
fn test_tag_count() -> Void {
  let mut idx = TagIndex::new();

  for i in 0..100u32 {
    idx.add(i, b"batch", b"1");
  }

  assert_eq!(idx.count(b"batch", b"1"), 100);
  assert_eq!(idx.count(b"batch", b"2"), 0);

  info!("tag count ok");
  OK
}

#[test]
fn test_tag_binary() -> Void {
  let mut idx = TagIndex::new();

  // Binary tag key/value 二进制标签键值
  let key = vec![0x00, 0xFF];
  let val = vec![0xAB, 0xCD];

  idx.add(42, &key, &val);

  let result = idx.get(&key, &val).unwrap();
  assert!(result.contains(42));

  info!("tag binary ok");
  OK
}
