use std::path::PathBuf;

use jdb_page::{PageStore, page_data, page_data_mut};

fn temp_path(name: &str) -> PathBuf {
  let mut p = std::env::temp_dir();
  p.push(format!("jdb_page_test_{}", fastrand::u64(..)));
  p.push(name);
  p
}

#[compio::test]
async fn basic() {
  let path = temp_path("basic.jdb");

  // Create new store
  let mut store = PageStore::open(&path).await.unwrap();
  assert_eq!(store.page_count(), 1); // header page

  // Alloc and write
  let id = store.alloc();
  assert_eq!(id, 1);

  let mut buf = jdb_alloc::AlignedBuf::page().unwrap();
  let data = page_data_mut(&mut buf);
  data[0..5].copy_from_slice(b"hello");

  store.write(id, &mut buf).await.unwrap();
  store.sync().await.unwrap();

  // Read back
  let buf2 = store.read(id).await.unwrap();
  assert_eq!(&page_data(&buf2)[0..5], b"hello");

  // Reopen
  drop(store);
  let store2 = PageStore::open(&path).await.unwrap();
  assert_eq!(store2.page_count(), 2);

  let buf3 = store2.read(id).await.unwrap();
  assert_eq!(&page_data(&buf3)[0..5], b"hello");

  // Cleanup
  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[compio::test]
async fn free_reuse() {
  let path = temp_path("free.jdb");
  let mut store = PageStore::open(&path).await.unwrap();

  let id1 = store.alloc();
  let id2 = store.alloc();
  assert_eq!(id1, 1);
  assert_eq!(id2, 2);

  store.free(id1);
  let id3 = store.alloc();
  assert_eq!(id3, id1); // reused

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[compio::test]
async fn checksum_error() {
  let path = temp_path("checksum.jdb");
  let mut store = PageStore::open(&path).await.unwrap();

  let id = store.alloc();
  let mut buf = jdb_alloc::AlignedBuf::page().unwrap();
  page_data_mut(&mut buf)[0] = 42;
  store.write(id, &mut buf).await.unwrap();
  store.sync().await.unwrap();
  drop(store);

  // Corrupt the file
  let raw = std::fs::read(&path).unwrap();
  let mut corrupted = raw.clone();
  corrupted[jdb_alloc::PAGE_SIZE + 20] ^= 0xFF; // flip a byte in data
  std::fs::write(&path, &corrupted).unwrap();

  // Reopen and read should fail
  let store2 = PageStore::open(&path).await.unwrap();
  let result = store2.read(id).await;
  assert!(matches!(result, Err(jdb_page::Error::Checksum { .. })));

  let _ = std::fs::remove_file(&path);
  let _ = std::fs::remove_dir_all(path.parent().unwrap());
}
