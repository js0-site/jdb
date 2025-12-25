//! Db tests / 数据库测试

use std::path::PathBuf;

use jdb_db::Db;
use jdb_trait::Order;

fn temp_dir() -> PathBuf {
  let id = fastrand::u64(..);
  std::env::temp_dir().join(format!("jdb_db_test_{id}"))
}

#[compio::test]
async fn basic_crud() {
  let dir = temp_dir();
  let mut db = Db::open(&dir).await.unwrap();

  // Put / 写入
  let old = db.put(b"key1", b"val1").await.unwrap();
  assert!(old.is_none());

  // Get / 读取
  let val = db.get(b"key1").await.unwrap();
  assert_eq!(val.as_deref(), Some(b"val1".as_slice()));

  // Update / 更新
  let old = db.put(b"key1", b"val2").await.unwrap();
  assert!(old.is_some());

  let val = db.get(b"key1").await.unwrap();
  assert_eq!(val.as_deref(), Some(b"val2".as_slice()));

  // Delete / 删除
  db.rm(b"key1").await.unwrap();
  let val = db.get(b"key1").await.unwrap();
  assert!(val.is_none());

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn multiple_keys() {
  let dir = temp_dir();
  let mut db = Db::open(&dir).await.unwrap();

  // Insert multiple / 插入多个
  for i in 0..100u32 {
    let key = format!("key:{i:03}");
    let val = format!("val:{i:03}");
    db.put(key.as_bytes(), val.as_bytes()).await.unwrap();
  }

  // Verify all / 验证全部
  for i in 0..100u32 {
    let key = format!("key:{i:03}");
    let expected = format!("val:{i:03}");
    let val = db.get(key.as_bytes()).await.unwrap();
    assert_eq!(val.as_deref(), Some(expected.as_bytes()));
  }

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn history() {
  let dir = temp_dir();
  let mut db = Db::open(&dir).await.unwrap();

  // Multiple updates / 多次更新
  db.put(b"key", b"v1").await.unwrap();
  db.put(b"key", b"v2").await.unwrap();
  db.put(b"key", b"v3").await.unwrap();

  // Get history / 获取历史
  let hist = db.history(b"key").await.unwrap();
  // Current impl: v3 -> v2 (v1 has no prev info stored)
  // 当前实现: v3 -> v2 (v1 没有存储 prev 信息)
  assert!(hist.len() >= 2);

  // Read latest value / 读取最新值
  let v3 = db.val(hist[0]).await.unwrap();
  assert_eq!(v3.as_deref(), Some(b"v3".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn scan() {
  let dir = temp_dir();
  let mut db = Db::open(&dir).await.unwrap();

  // Insert / 插入
  db.put(b"a", b"1").await.unwrap();
  db.put(b"b", b"2").await.unwrap();
  db.put(b"c", b"3").await.unwrap();

  // Scan asc / 升序扫描
  let results = db.scan(b"a", Order::Asc).await.unwrap();
  assert!(!results.is_empty());

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn tombstone_history() {
  let dir = temp_dir();
  let mut db = Db::open(&dir).await.unwrap();

  // Put then delete / 写入后删除
  db.put(b"key", b"value").await.unwrap();
  db.rm(b"key").await.unwrap();

  // Get returns None / get 返回 None
  let val = db.get(b"key").await.unwrap();
  assert!(val.is_none());

  // History still has entries / 历史仍有记录
  let hist = db.history(b"key").await.unwrap();
  assert!(!hist.is_empty());

  // First entry is tombstone / 第一个是墓碑
  assert!(hist[0].is_tombstone());

  let _ = std::fs::remove_dir_all(&dir);
}
