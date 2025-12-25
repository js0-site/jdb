use std::path::PathBuf;

use jdb_page::PageStore;
use jdb_trait::ValRef;
use jdb_tree::BTree;

fn temp_path(name: &str) -> PathBuf {
  let mut p = std::env::temp_dir();
  p.push(format!("jdb_tree_more_test_{}", fastrand::u64(..)));
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
async fn delete_internal() {
  let path = temp_path("del_internal.jdb");
  let store = PageStore::open(&path).await.unwrap();
  let mut tree = BTree::new(store).await.unwrap();

  // Create 2-level tree
  for i in 0..200u64 {
    let key = format!("key{:03}", i);
    tree.put(key.as_bytes(), vref(i)).await.unwrap();
  }

  // Delete from different leaves to cover internal recursion
  for i in 0..200u64 {
    let key = format!("key{:03}", i);
    let (_, old) = tree.del(key.as_bytes()).await.unwrap();
    assert_eq!(old.map(|v| v.offset), Some(i));
  }

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[compio::test]
async fn leaf_ops() {
  let path = temp_path("leaf.jdb");
  let store = PageStore::open(&path).await.unwrap();
  let mut tree = BTree::new(store).await.unwrap();

  tree.put(b"a", vref(1)).await.unwrap();
  let (pid, _leaf) = tree.find_leaf(b"a").await.unwrap();

  let leaf2 = tree.read_leaf(pid).await.unwrap();
  assert_eq!(leaf2.vals[0].offset, 1);

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}
