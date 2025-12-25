//! VLog tests / VLog 测试

use std::path::PathBuf;

use jdb_vlog::VLog;

fn temp_dir() -> PathBuf {
  let id = fastrand::u64(..);
  std::env::temp_dir().join(format!("jdb_vlog_test_{id}"))
}

#[compio::test]
async fn open_create() {
  let dir = temp_dir();
  let vlog = VLog::open(&dir).await.unwrap();
  assert_eq!(vlog.active_id(), 1);
  assert_eq!(vlog.active_size(), 0);

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn append_get() {
  let dir = temp_dir();
  let vlog = VLog::open(&dir).await.unwrap();

  // Append value / 追加值
  let key = b"hello";
  let val = b"world";
  let vref = vlog.append(key, val, None).await.unwrap();

  assert_eq!(vref.file_id, 1);
  assert!(!vref.is_tombstone());
  assert!(!vref.has_prev());

  // Get value / 获取值
  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(val.as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn append_with_prev() {
  let dir = temp_dir();
  let vlog = VLog::open(&dir).await.unwrap();

  // First value / 第一个值
  let key = b"key1";
  let vref1 = vlog.append(key, b"val1", None).await.unwrap();
  assert!(!vref1.has_prev());

  // Second value with prev / 带前驱的第二个值
  let vref2 = vlog.append(key, b"val2", Some(&vref1)).await.unwrap();
  assert!(vref2.has_prev());
  assert_eq!(vref2.prev_file_id, vref1.file_id);
  assert_eq!(vref2.prev_offset, vref1.offset);

  // Get both / 获取两个值
  let got1 = vlog.get(&vref1).await.unwrap();
  let got2 = vlog.get(&vref2).await.unwrap();
  assert_eq!(got1.as_deref(), Some(b"val1".as_slice()));
  assert_eq!(got2.as_deref(), Some(b"val2".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn tombstone() {
  let dir = temp_dir();
  let vlog = VLog::open(&dir).await.unwrap();

  // Append value / 追加值
  let key = b"key";
  let vref1 = vlog.append(key, b"value", None).await.unwrap();

  // Append tombstone / 追加墓碑
  let vref2 = vlog.append_tombstone(key, Some(&vref1)).await.unwrap();
  assert!(vref2.is_tombstone());
  assert!(vref2.has_prev());

  // Get tombstone returns None / 获取墓碑返回 None
  let got = vlog.get(&vref2).await.unwrap();
  assert!(got.is_none());

  // Can still get prev value / 仍可获取前驱值
  let got1 = vlog.get(&vref1).await.unwrap();
  assert_eq!(got1.as_deref(), Some(b"value".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn reopen() {
  let dir = temp_dir();

  // Write some data / 写入数据
  let vref = {
    let vlog = VLog::open(&dir).await.unwrap();
    let vref = vlog.append(b"key", b"value", None).await.unwrap();
    vlog.sync().await.unwrap();
    vref
  };

  // Reopen and read / 重新打开并读取
  let vlog = VLog::open(&dir).await.unwrap();
  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(b"value".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn large_value() {
  let dir = temp_dir();
  let vlog = VLog::open(&dir).await.unwrap();

  // Large value (8KB) / 大值
  let key = b"large";
  let val = vec![0x42u8; 8192];
  let vref = vlog.append(key, &val, None).await.unwrap();

  let got = vlog.get(&vref).await.unwrap();
  assert_eq!(got.as_deref(), Some(val.as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}
