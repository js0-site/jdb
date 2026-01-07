//! Flush property tests
//! Flush 属性测试
//!
//! Feature: jdb-code-review, Property 1: 恢复一致性
//! *对于任意* 写入序列，flush 后关闭再打开，所有数据应可读取
//! **验证: 需求 2.3**

use std::collections::BTreeMap;

use jdb::{Conf, Db};
use jdb_mem::Mem;
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

  /// Property 1: Recovery consistency after flush
  /// 恢复一致性：flush 后关闭再打开，数据应可读取
  ///
  /// **Validates: Requirements 2.3**
  #[test]
  fn prop_flush_recovery_consistency(kvs in kv_strategy()) {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path();

      // Expected state after writes / 写入后的预期状态
      let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

      // Phase 1: Open, write, flush, close
      // 阶段1：打开、写入、flush、关闭
      {
        // Use small mem threshold to trigger flush
        // 使用小的内存阈值触发 flush
        let conf = [Conf::MemThreshold(1024)];
        let mut db = Db::open(path, &conf).await.expect("open db");

        for (key, val) in &kvs {
          db.put(key, val).await.expect("put");
          expected.insert(key.clone(), val.clone());
        }

        // Trigger flush by freezing current memtable
        // 通过冻结当前内存表触发 flush
        if !db.mem.is_empty() {
          // Manually trigger flush
          // 手动触发 flush
          let old_mem = std::mem::replace(&mut db.mem, Mem::new());
          db.frozen.push(old_mem);
        }

        // Flush all frozen memtables to SSTable
        // 将所有冻结的内存表刷写到 SSTable
        db.flush_all().await.expect("flush_all");

        // Sync WAL
        // 同步 WAL
        db.wal.flush().await.expect("wal flush");
        db.wal.sync().await.expect("wal sync");
      }

      // Phase 2: Reopen and verify
      // 阶段2：重新打开并验证
      {
        let mut db = Db::open(path, &[]).await.expect("reopen db");

        // Verify all keys exist and values match
        // 验证所有键存在且值匹配
        for (key, expected_val) in &expected {
          let actual = db.get(key).await.expect("get");
          prop_assert!(
            actual.is_some(),
            "Key {:?} not found after flush recovery",
            key
          );
          prop_assert_eq!(
            &actual.unwrap(),
            expected_val,
            "Value mismatch for key {:?}",
            key
          );
        }
      }

      Ok(())
    })?;
  }
}
