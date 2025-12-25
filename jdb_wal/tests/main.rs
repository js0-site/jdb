//! WAL tests / WAL 测试

use std::path::PathBuf;

use jdb_wal::{Record, RecordType, Wal};

fn temp_dir() -> PathBuf {
  let id = fastrand::u64(..);
  std::env::temp_dir().join(format!("jdb_wal_test_{id}"))
}

#[compio::test]
async fn open_create() {
  let dir = temp_dir();
  let wal = Wal::open(&dir).await.unwrap();
  assert_eq!(wal.active_id(), 1);
  assert_eq!(wal.size(), 0);

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn append_recover() {
  let dir = temp_dir();

  // Write records / 写入记录
  {
    let wal = Wal::open(&dir).await.unwrap();
    wal
      .append(&Record::put(1, b"key1".as_slice(), b"val1".as_slice()))
      .await
      .unwrap();
    wal
      .append(&Record::put(1, b"key2".as_slice(), b"val2".as_slice()))
      .await
      .unwrap();
    wal
      .append(&Record::del(1, b"key1".as_slice()))
      .await
      .unwrap();
    wal.append(&Record::commit(1)).await.unwrap();
    wal.sync().await.unwrap();
  }

  // Recover / 恢复
  {
    let wal = Wal::open(&dir).await.unwrap();
    let records = wal.recover().await.unwrap();
    assert_eq!(records.len(), 4);

    assert_eq!(records[0].typ, RecordType::Put);
    assert_eq!(records[0].key.as_ref(), b"key1");
    assert_eq!(records[0].val.as_ref(), b"val1");

    assert_eq!(records[1].typ, RecordType::Put);
    assert_eq!(records[1].key.as_ref(), b"key2");

    assert_eq!(records[2].typ, RecordType::Del);
    assert_eq!(records[2].key.as_ref(), b"key1");

    assert_eq!(records[3].typ, RecordType::Commit);
  }

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn clear() {
  let dir = temp_dir();

  let wal = Wal::open(&dir).await.unwrap();
  wal
    .append(&Record::put(1, b"k".as_slice(), b"v".as_slice()))
    .await
    .unwrap();
  wal.sync().await.unwrap();

  wal.clear().await.unwrap();
  assert_eq!(wal.active_id(), 1);
  assert_eq!(wal.size(), 0);

  let records = wal.recover().await.unwrap();
  assert!(records.is_empty());

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn multi_db() {
  let dir = temp_dir();

  let wal = Wal::open(&dir).await.unwrap();
  wal
    .append(&Record::put(1, b"k1".as_slice(), b"v1".as_slice()))
    .await
    .unwrap();
  wal
    .append(&Record::put(2, b"k2".as_slice(), b"v2".as_slice()))
    .await
    .unwrap();
  wal.append(&Record::commit(1)).await.unwrap();
  wal.append(&Record::commit(2)).await.unwrap();
  wal.sync().await.unwrap();

  let records = wal.recover().await.unwrap();
  assert_eq!(records.len(), 4);
  assert_eq!(records[0].db_id, 1);
  assert_eq!(records[1].db_id, 2);

  let _ = std::fs::remove_dir_all(&dir);
}
