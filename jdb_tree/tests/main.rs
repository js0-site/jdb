//! B+ Tree tests / B+ 树测试

use std::path::PathBuf;

use jdb_page::PageStore;
use jdb_trait::ValRef;
use jdb_tree::BTree;

fn temp_path(name: &str) -> PathBuf {
  let mut p = std::env::temp_dir();
  p.push(format!("jdb_tree_test_{}", fastrand::u64(..)));
  p.push(name);
  p
}

fn vref(n: u64) -> ValRef {
  ValRef {
    file_id: 1,
    offset: n,
    prev_file_id: 0,
    prev_offset: 0,
  }
}

#[compio::test]
async fn basic_crud() {
  let path = temp_path("basic.jdb");
  let store = PageStore::open(&path).await.unwrap();
  let mut tree = BTree::new(store).await.unwrap();

  // Insert / 插入
  tree.put(b"key1", vref(100)).await.unwrap();
  tree.put(b"key2", vref(200)).await.unwrap();
  tree.put(b"key3", vref(300)).await.unwrap();

  // Get / 获取
  let v1 = tree.get(b"key1").await.unwrap();
  let v2 = tree.get(b"key2").await.unwrap();
  let v3 = tree.get(b"key3").await.unwrap();
  let v4 = tree.get(b"key4").await.unwrap();

  assert_eq!(v1.map(|v| v.offset), Some(100));
  assert_eq!(v2.map(|v| v.offset), Some(200));
  assert_eq!(v3.map(|v| v.offset), Some(300));
  assert!(v4.is_none());

  // Update / 更新
  tree.put(b"key2", vref(250)).await.unwrap();
  let v2 = tree.get(b"key2").await.unwrap();
  assert_eq!(v2.map(|v| v.offset), Some(250));

  // Delete / 删除
  let (_, old) = tree.del(b"key2").await.unwrap();
  assert_eq!(old.map(|v| v.offset), Some(250));

  let v2 = tree.get(b"key2").await.unwrap();
  assert!(v2.is_none());

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[compio::test]
async fn prefix_compression() {
  let path = temp_path("prefix.jdb");
  let store = PageStore::open(&path).await.unwrap();
  let mut tree = BTree::new(store).await.unwrap();

  // Insert keys with common prefix / 插入有公共前缀的键
  tree.put(b"user:1001", vref(1)).await.unwrap();
  tree.put(b"user:1002", vref(2)).await.unwrap();
  tree.put(b"user:1003", vref(3)).await.unwrap();

  // Verify / 验证
  let v1 = tree.get(b"user:1001").await.unwrap();
  let v2 = tree.get(b"user:1002").await.unwrap();
  let v3 = tree.get(b"user:1003").await.unwrap();

  assert_eq!(v1.map(|v| v.offset), Some(1));
  assert_eq!(v2.map(|v| v.offset), Some(2));
  assert_eq!(v3.map(|v| v.offset), Some(3));

  // Check leaf prefix / 检查叶子前缀
  let (_, leaf) = tree.find_leaf(b"user:1001").await.unwrap();
  assert!(!leaf.prefix.is_empty());

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[compio::test]
async fn many_inserts() {
  let path = temp_path("many.jdb");
  let store = PageStore::open(&path).await.unwrap();
  let mut tree = BTree::new(store).await.unwrap();

  // Insert many keys to trigger splits / 插入大量键触发分裂
  for i in 0..500u64 {
    let key = format!("key:{i:05}");
    tree.put(key.as_bytes(), vref(i)).await.unwrap();
  }

  // Verify all / 验证全部
  for i in 0..500u64 {
    let key = format!("key:{i:05}");
    let v = tree.get(key.as_bytes()).await.unwrap();
    assert_eq!(v.map(|v| v.offset), Some(i), "key={key}");
  }

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[compio::test]
async fn cow_preserves_old() {
  let path = temp_path("cow.jdb");
  let store = PageStore::open(&path).await.unwrap();
  let mut tree = BTree::new(store).await.unwrap();

  // Insert and get root / 插入并获取根
  tree.put(b"a", vref(1)).await.unwrap();
  tree.put(b"b", vref(2)).await.unwrap();
  let root1 = tree.root();

  // More inserts / 更多插入
  tree.put(b"c", vref(3)).await.unwrap();
  let root2 = tree.root();

  // Roots should differ (CoW) / 根应不同
  assert_ne!(root1, root2);

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}
