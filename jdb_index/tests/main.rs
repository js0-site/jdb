use aok::{OK, Void};
use jdb_index::BTree;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_btree_insert_get() -> Void {
  let mut tree = BTree::new();

  tree.insert(b"key1".to_vec(), b"val1".to_vec());
  tree.insert(b"key2".to_vec(), b"val2".to_vec());
  tree.insert(b"key3".to_vec(), b"val3".to_vec());

  assert_eq!(tree.get(b"key1"), Some(b"val1".to_vec()));
  assert_eq!(tree.get(b"key2"), Some(b"val2".to_vec()));
  assert_eq!(tree.get(b"key3"), Some(b"val3".to_vec()));
  assert_eq!(tree.get(b"key4"), None);

  info!("btree insert/get ok");
  OK
}

#[test]
fn test_btree_update() -> Void {
  let mut tree = BTree::new();

  tree.insert(b"key".to_vec(), b"val1".to_vec());
  assert_eq!(tree.get(b"key"), Some(b"val1".to_vec()));

  tree.insert(b"key".to_vec(), b"val2".to_vec());
  assert_eq!(tree.get(b"key"), Some(b"val2".to_vec()));

  info!("btree update ok");
  OK
}

#[test]
fn test_btree_delete() -> Void {
  let mut tree = BTree::new();

  tree.insert(b"key1".to_vec(), b"val1".to_vec());
  tree.insert(b"key2".to_vec(), b"val2".to_vec());

  assert!(tree.delete(b"key1"));
  assert_eq!(tree.get(b"key1"), None);
  assert_eq!(tree.get(b"key2"), Some(b"val2".to_vec()));

  assert!(!tree.delete(b"key1")); // Already deleted 已删除

  info!("btree delete ok");
  OK
}

#[test]
fn test_btree_range() -> Void {
  let mut tree = BTree::new();

  tree.insert(b"a".to_vec(), b"1".to_vec());
  tree.insert(b"b".to_vec(), b"2".to_vec());
  tree.insert(b"c".to_vec(), b"3".to_vec());
  tree.insert(b"d".to_vec(), b"4".to_vec());
  tree.insert(b"e".to_vec(), b"5".to_vec());

  let result = tree.range(b"b", b"d");
  assert_eq!(result.len(), 3);
  assert_eq!(result[0], (b"b".to_vec(), b"2".to_vec()));
  assert_eq!(result[1], (b"c".to_vec(), b"3".to_vec()));
  assert_eq!(result[2], (b"d".to_vec(), b"4".to_vec()));

  info!("btree range ok");
  OK
}

#[test]
fn test_btree_many_keys() -> Void {
  let mut tree = BTree::new();

  // Insert many keys 插入多个键
  for i in 0..200u32 {
    let key = format!("key{i:04}").into_bytes();
    let val = format!("val{i:04}").into_bytes();
    tree.insert(key, val);
  }

  // Verify 验证
  for i in 0..200u32 {
    let key = format!("key{i:04}").into_bytes();
    let val = format!("val{i:04}").into_bytes();
    assert_eq!(tree.get(&key), Some(val));
  }

  info!("btree many keys ok");
  OK
}

#[test]
fn test_btree_binary_keys() -> Void {
  let mut tree = BTree::new();

  // Binary keys 二进制键
  let k1 = vec![0x00, 0xFF, 0xAB];
  let k2 = vec![0x00, 0xFF, 0xAC];
  let k3 = vec![0x01, 0x00, 0x00];

  tree.insert(k1.clone(), b"v1".to_vec());
  tree.insert(k2.clone(), b"v2".to_vec());
  tree.insert(k3.clone(), b"v3".to_vec());

  assert_eq!(tree.get(&k1), Some(b"v1".to_vec()));
  assert_eq!(tree.get(&k2), Some(b"v2".to_vec()));
  assert_eq!(tree.get(&k3), Some(b"v3".to_vec()));

  info!("btree binary keys ok");
  OK
}
