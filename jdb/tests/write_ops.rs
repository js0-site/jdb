//! Write operations property tests
//! 写操作属性测试

use std::collections::BTreeMap;

use jdb::Db;
use proptest::prelude::*;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Generate random key-value pairs
/// 生成随机键值对
fn kv_strategy() -> impl Strategy<Value = Vec<(Vec<u8>, Vec<u8>)>> {
  prop::collection::vec(
    (
      prop::collection::vec(any::<u8>(), 1..64), // key: 1-64 bytes
      prop::collection::vec(any::<u8>(), 0..256), // val: 0-256 bytes
    ),
    1..50, // 1-50 pairs
  )
}

/// Run async test in compio runtime
fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new()
    .expect("create runtime")
    .block_on(f)
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// Feature: jdb-integration, Property 1: 读写一致性
  /// *对于任意* 键值对 (k, v)，写入后立即读取应返回相同的值 v
  /// **验证: 需求 2.1, 3.4**
  #[test]
  fn prop_read_write_consistency(kvs in kv_strategy()) {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path();

      let mut db = Db::open(path, &[]).await.expect("open db");

      // Write all key-value pairs / 写入所有键值对
      let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
      for (key, val) in &kvs {
        db.put(key, val).await.expect("put");
        expected.insert(key.clone(), val.clone());
      }

      // Flush to ensure data is written / 刷新确保数据写入
      db.wal.flush().await.expect("flush");

      // Read back and verify / 读取并验证
      for (key, expected_val) in &expected {
        let actual = db.get(key).await.expect("get");
        prop_assert!(
          actual.is_some(),
          "Key {:?} not found after write",
          key
        );
        prop_assert_eq!(
          &actual.unwrap(),
          expected_val,
          "Value mismatch for key {:?}",
          key
        );
      }

      Ok(())
    })?;
  }
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// Feature: jdb-integration, Property 2: 删除语义
  /// *对于任意* 键 k，先写入再删除后，读取应返回 None
  /// **验证: 需求 2.2, 3.3**
  #[test]
  fn prop_delete_semantics(kvs in kv_strategy()) {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path();

      let mut db = Db::open(path, &[]).await.expect("open db");

      // Write all key-value pairs / 写入所有键值对
      for (key, val) in &kvs {
        db.put(key, val).await.expect("put");
      }

      // Flush to ensure data is written / 刷新确保数据写入
      db.wal.flush().await.expect("flush");

      // Delete all keys / 删除所有键
      for (key, _) in &kvs {
        db.rm(key).await.expect("rm");
      }

      // Flush again / 再次刷新
      db.wal.flush().await.expect("flush");

      // Verify all keys return None / 验证所有键返回 None
      for (key, _) in &kvs {
        let actual = db.get(key).await.expect("get");
        prop_assert!(
          actual.is_none(),
          "Key {:?} should be None after delete, got {:?}",
          key,
          actual
        );
      }

      Ok(())
    })?;
  }
}
