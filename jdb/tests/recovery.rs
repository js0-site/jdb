//! Recovery consistency property tests
//! 恢复一致性属性测试
//!
//! Feature: jdb-integration, Property 8: 恢复一致性
//! *对于任意* 写入序列，关闭后重新打开，所有数据应可读取
//! **验证: 需求 1.2, 7.1, 7.2**

#![allow(clippy::await_holding_refcell_ref)]

use std::collections::BTreeMap;

use jdb::Db;
use jdb_base::{
  Pos,
  table::{Table, TableMut},
};
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

  /// Property 8: Recovery consistency
  /// 恢复一致性：写入后关闭再打开，数据应可读取
  #[test]
  fn prop_recovery_consistency(kvs in kv_strategy()) {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path();

      // Expected state after writes / 写入后的预期状态
      let mut expected: BTreeMap<Vec<u8>, Pos> = BTreeMap::new();

      // Phase 1: Open, write, close / 阶段1：打开、写入、关闭
      {
        let mut db = Db::open(path, &[]).await.expect("open db");

        // Save checkpoint BEFORE writing data
        // 在写入数据之前保存检查点
        {
          let mut ckp = db.ckp.borrow_mut();
          ckp.set_wal_ptr(db.wal.cur_id(), db.wal.cur_pos())
            .await
            .expect("save ckp before write");
        }

        for (key, val) in &kvs {
          let pos = db.wal.put(key, val).await.expect("put");
          db.mem.put(key.clone(), pos);
          expected.insert(key.clone(), pos);
        }

        // Flush and sync / 刷新并同步
        db.wal.flush().await.expect("flush");
        db.wal.sync().await.expect("sync");
      }

      // Phase 2: Reopen and verify / 阶段2：重新打开并验证
      {
        let db = Db::open(path, &[]).await.expect("reopen db");

        // Verify all keys exist in memtable / 验证所有键存在于内存表
        for (key, expected_pos) in &expected {
          let actual_pos = db.mem.get(key);
          prop_assert!(
            actual_pos.is_some(),
            "Key {:?} not found after recovery",
            key
          );
          prop_assert_eq!(
            actual_pos.unwrap(),
            *expected_pos,
            "Pos mismatch for key {:?}",
            key
          );
        }

        // Verify memtable size matches / 验证内存表大小匹配
        prop_assert_eq!(
          db.mem.len(),
          expected.len(),
          "Memtable size mismatch"
        );
      }

      Ok(())
    })?;
  }
}
