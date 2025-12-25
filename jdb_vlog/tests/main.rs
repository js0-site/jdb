//! VLog tests / VLog 测试

use defer_lite::defer;
use jdb_vlog::VLog;
use tempdir::TempDir;

#[compio::test]
async fn open_create() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();
  assert_eq!(vlog.active_id(), 1);
  assert_eq!(vlog.active_size(), 0);
}

#[compio::test]
async fn append_get() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();

  let val = b"world";
  let vref = vlog.append(val, None).await.unwrap();

  assert_eq!(vref.file_id, 1);
  assert!(!vref.is_tombstone());
  assert!(!vref.has_prev());

  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(val.as_slice()));
}

#[compio::test]
async fn append_with_prev() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();

  let vref1 = vlog.append(b"val1", None).await.unwrap();
  assert!(!vref1.has_prev());

  let vref2 = vlog.append(b"val2", Some(&vref1)).await.unwrap();
  assert!(vref2.has_prev());
  assert_eq!(vref2.prev_file_id, vref1.file_id);
  assert_eq!(vref2.prev_offset, vref1.offset);

  let got1 = vlog.get(&vref1).await.unwrap();
  let got2 = vlog.get(&vref2).await.unwrap();
  assert_eq!(got1.as_deref(), Some(b"val1".as_slice()));
  assert_eq!(got2.as_deref(), Some(b"val2".as_slice()));
}

#[compio::test]
async fn tombstone() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();

  let vref1 = vlog.append(b"value", None).await.unwrap();
  let vref2 = vlog.append_tombstone(Some(&vref1)).await.unwrap();

  assert!(vref2.is_tombstone());
  assert!(vref2.has_prev());

  let got = vlog.get(&vref2).await.unwrap();
  assert!(got.is_none());

  let got1 = vlog.get(&vref1).await.unwrap();
  assert_eq!(got1.as_deref(), Some(b"value".as_slice()));
}

#[compio::test]
async fn reopen() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vref = {
    let vlog = VLog::open(dir.path()).await.unwrap();
    let vref = vlog.append(b"value", None).await.unwrap();
    vlog.sync().await.unwrap();
    vref
  };

  let vlog = VLog::open(dir.path()).await.unwrap();
  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(b"value".as_slice()));
}

#[compio::test]
async fn small_value() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();

  // Small value (8KB, inline) / 小值（内联）
  let val = vec![0x42u8; 8192];
  let vref = vlog.append(&val, None).await.unwrap();

  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(val.as_slice()));
}

#[compio::test]
async fn large_value() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();

  // Large value (2MB, external) / 大值（外部文件）
  let val = vec![0xABu8; 2 * 1024 * 1024];
  let vref = vlog.append(&val, None).await.unwrap();

  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(val.as_slice()));

  // Check blob dir exists / 检查 blob 目录存在
  let blob_dir = dir.path().join("blob");
  assert!(blob_dir.exists());
}

#[compio::test]
async fn large_value_with_history() {
  let dir = TempDir::new("vlog_test").unwrap();
  defer! { let _ = std::fs::remove_dir_all(dir.path()); }

  let vlog = VLog::open(dir.path()).await.unwrap();

  // First large value / 第一个大值
  let val1 = vec![0x11u8; 1024 * 1024 + 1];
  let vref1 = vlog.append(&val1, None).await.unwrap();

  // Second large value with prev / 带前驱的第二个大值
  let val2 = vec![0x22u8; 1024 * 1024 + 2];
  let vref2 = vlog.append(&val2, Some(&vref1)).await.unwrap();

  assert!(vref2.has_prev());

  let got1 = vlog.get(&vref1).await.unwrap();
  let got2 = vlog.get(&vref2).await.unwrap();
  assert_eq!(got1.as_deref(), Some(val1.as_slice()));
  assert_eq!(got2.as_deref(), Some(val2.as_slice()));
}
