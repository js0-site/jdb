use std::path::PathBuf;

use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::{File, exists, ls, mkdir, remove, rename, size, sync_dir};

fn temp_path(name: &str) -> PathBuf {
  let mut p = std::env::temp_dir();
  p.push(format!("jdb_fs_test_{}", fastrand::u64(..)));
  p.push(name);
  p
}

#[compio::test]
async fn meta() {
  let root = temp_path("meta");
  let dir = root.join("subdir");
  let file = dir.join("test.txt");

  // mkdir
  mkdir(&dir).await.unwrap();
  assert!(exists(&dir));

  // sync_dir
  sync_dir(&dir).await.unwrap();

  // write file manually to test ls/size
  std::fs::write(&file, b"hello").unwrap();
  assert!(exists(&file));

  // ls
  let file_li = ls(&dir).await.unwrap();
  assert_eq!(file_li.len(), 1);
  assert_eq!(file_li[0], file);

  // size
  assert_eq!(size(&file).await.unwrap(), 5);

  // rename
  let file2 = dir.join("test2.txt");
  rename(&file, &file2).await.unwrap();
  assert!(!exists(&file));
  assert!(exists(&file2));

  // remove
  remove(&file2).await.unwrap();
  assert!(!exists(&file2));

  // cleanup
  let _ = std::fs::remove_dir_all(&root);
}

#[compio::test]
async fn file_ops() {
  let path = temp_path("file_ops.dat");
  let dir = path.parent().unwrap();
  mkdir(dir).await.unwrap();

  // create
  let file = File::create(&path).await.unwrap();
  assert_eq!(file.size().await.unwrap(), 0);

  // write_at
  let mut buf = AlignedBuf::page().unwrap();
  buf.clear();
  buf.extend(&[0xAA; PAGE_SIZE]).unwrap();
  let buf = file.write_at(buf, 0).await.unwrap();
  assert_eq!(file.size().await.unwrap(), PAGE_SIZE as u64);

  // sync
  file.sync_data().await.unwrap();
  file.sync_all().await.unwrap();

  // read_at
  let rbuf = AlignedBuf::page().unwrap();
  let rbuf = file.read_at(rbuf, 0).await.unwrap();
  assert_eq!(&rbuf[..], &buf[..]);

  // open_rw
  drop(file);
  let file = File::open_rw(&path).await.unwrap();
  assert_eq!(file.size().await.unwrap(), PAGE_SIZE as u64);

  // open (read-only)
  let file_ro = File::open(&path).await.unwrap();
  let rbuf2 = AlignedBuf::page().unwrap();
  let _ = file_ro.read_at(rbuf2, 0).await.unwrap();

  // cleanup
  drop(file);
  drop(file_ro);
  remove(&path).await.unwrap();
  let _ = std::fs::remove_dir_all(dir);
}

#[compio::test]
async fn file_adv() {
  let path = temp_path("file_adv.dat");
  let dir = path.parent().unwrap();
  mkdir(dir).await.unwrap();

  let file = File::create(&path).await.unwrap();

  // preallocate
  file.preallocate(PAGE_SIZE as u64 * 2).await.unwrap();
  // Note: preallocate might not change reported size on all systems, but should work.

  // truncate
  file.truncate(PAGE_SIZE as u64).await.unwrap();
  assert_eq!(file.size().await.unwrap(), PAGE_SIZE as u64);

  // open_wal
  drop(file);
  let _file = File::open_wal(&path).await.unwrap();

  // cleanup
  remove(&path).await.unwrap();
  let _ = std::fs::remove_dir_all(dir);
}

#[compio::test]
async fn alignment_error() {
  let path = temp_path("align.dat");
  let dir = path.parent().unwrap();
  mkdir(dir).await.unwrap();

  let file = File::create(&path).await.unwrap();
  let buf = AlignedBuf::page().unwrap();

  // Unaligned offset
  let res = file.write_at(buf, 100).await;
  assert!(matches!(res, Err(jdb_fs::Error::Alignment { .. })));

  let _buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  // Unaligned len (if buf.len() is not page aligned)
  // Actually check_align uses buf.buf_len() for write and buf_capacity() for read.

  // For read_at:
  let rbuf = AlignedBuf::with_cap(PAGE_SIZE + 100).unwrap();
  let res = file.read_at(rbuf, 0).await;
  assert!(matches!(res, Err(jdb_fs::Error::Alignment { .. })));

  // cleanup
  remove(&path).await.unwrap();
  let _ = std::fs::remove_dir_all(dir);
}
