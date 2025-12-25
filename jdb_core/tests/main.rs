//! Jdb tests / Jdb 测试

use std::path::PathBuf;

use jdb_core::Jdb;
use jdb_trait::Order;

fn temp_dir() -> PathBuf {
  let id = fastrand::u64(..);
  std::env::temp_dir().join(format!("jdb_core_test_{id}"))
}

#[compio::test]
async fn open_create() {
  let dir = temp_dir();
  let jdb = Jdb::open(&dir).await.unwrap();
  assert_eq!(jdb.next_id(), 1);

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn create_table() {
  let dir = temp_dir();
  let mut jdb = Jdb::open(&dir).await.unwrap();

  // Create table / 创建表
  let mut tbl = jdb.table(1, &[]).await.unwrap();

  // Write data / 写入数据
  tbl.put(b"key", b"value").await.unwrap();

  // Read back / 读取
  let val = tbl.get(b"key").await.unwrap();
  assert_eq!(val.as_deref(), Some(b"value".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn multiple_tables() {
  let dir = temp_dir();
  let mut jdb = Jdb::open(&dir).await.unwrap();

  // Create multiple tables / 创建多个表
  let mut tbl1 = jdb.table(1, &[]).await.unwrap();
  let mut tbl2 = jdb.table(2, &[]).await.unwrap();

  // Write to each / 分别写入
  tbl1.put(b"key", b"val1").await.unwrap();
  tbl2.put(b"key", b"val2").await.unwrap();

  // Verify isolation / 验证隔离
  let v1 = tbl1.get(b"key").await.unwrap();
  let v2 = tbl2.get(b"key").await.unwrap();
  assert_eq!(v1.as_deref(), Some(b"val1".as_slice()));
  assert_eq!(v2.as_deref(), Some(b"val2".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn fork_table() {
  let dir = temp_dir();
  let mut jdb = Jdb::open(&dir).await.unwrap();

  // Create and write / 创建并写入
  let mut tbl1 = jdb.table(1, &[]).await.unwrap();
  tbl1.put(b"key", b"original").await.unwrap();
  tbl1.sync().await.unwrap();
  jdb.commit_table(1, &tbl1);
  drop(tbl1);

  // Fork / Fork
  let forked = jdb.fork(1).await.unwrap();
  assert!(forked.is_some());

  let mut tbl_fork = forked.unwrap();

  // Modify fork / 修改 fork
  tbl_fork.put(b"key", b"modified").await.unwrap();

  // Original unchanged / 原始不变
  let tbl1 = jdb.table(1, &[]).await.unwrap();
  let v1 = tbl1.get(b"key").await.unwrap();
  assert_eq!(v1.as_deref(), Some(b"original".as_slice()));

  // Fork has new value / Fork 有新值
  let v2 = tbl_fork.get(b"key").await.unwrap();
  assert_eq!(v2.as_deref(), Some(b"modified".as_slice()));

  let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn scan_tables() {
  let dir = temp_dir();
  let mut jdb = Jdb::open(&dir).await.unwrap();

  // Create tables / 创建表
  jdb.table(1, &[]).await.unwrap();
  jdb.table(3, &[]).await.unwrap();
  jdb.table(5, &[]).await.unwrap();

  // Scan asc / 升序扫描
  let ids = jdb.scan(1, Order::Asc);
  assert_eq!(ids, vec![1, 3, 5]);

  // Scan desc / 降序扫描
  let ids = jdb.scan(5, Order::Desc);
  assert_eq!(ids, vec![5, 3, 1]);

  // Scan from middle / 从中间扫描
  let ids = jdb.scan(3, Order::Asc);
  assert_eq!(ids, vec![3, 5]);

  let _ = std::fs::remove_dir_all(&dir);
}
