//! Property-based tests for Snapshot lifecycle
//! Snapshot 生命周期属性测试

use std::{cell::RefCell, rc::Rc};

use jdb_base::table::Meta;
use jdb_level::{Level, RefCountMap, Snapshot};
use proptest::prelude::*;

/// Mock table for testing
/// 测试用模拟表
#[derive(Debug, Clone)]
struct MockTable {
  id: u64,
  min_key: Vec<u8>,
  max_key: Vec<u8>,
  size: u64,
}

impl MockTable {
  fn new(id: u64) -> Self {
    let key = id.to_be_bytes().to_vec();
    Self {
      id,
      min_key: key.clone(),
      max_key: key,
      size: 1024,
    }
  }
}

impl Meta for MockTable {
  fn id(&self) -> u64 {
    self.id
  }

  fn min_key(&self) -> &[u8] {
    &self.min_key
  }

  fn max_key(&self) -> &[u8] {
    &self.max_key
  }

  fn size(&self) -> u64 {
    self.size
  }

  fn count(&self) -> u64 {
    10
  }

  fn rm_count(&self) -> u64 {
    0
  }
}

/// Operation on Snapshot
/// Snapshot 操作
#[derive(Debug, Clone)]
enum SnapshotOp {
  /// Create a new snapshot
  /// 创建新快照
  Create,
  /// Clone an existing snapshot (index into live snapshots)
  /// 克隆现有快照（索引到活跃快照）
  Clone(usize),
  /// Drop a snapshot (index into live snapshots)
  /// 删除快照（索引到活跃快照）
  Drop(usize),
}

/// Generate random snapshot operations
/// 生成随机快照操作
fn snapshot_op_strategy() -> impl Strategy<Value = SnapshotOp> {
  prop_oneof![
    3 => Just(SnapshotOp::Create),
    2 => (0..10usize).prop_map(SnapshotOp::Clone),
    2 => (0..10usize).prop_map(SnapshotOp::Drop),
  ]
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// Property 4: Snapshot reference count round-trip consistency
  /// For any Snapshot, creating it increments refcount by 1, cloning it increments by 1,
  /// and dropping it decrements by 1. The refcount for a version SHALL equal the number
  /// of live Snapshot instances for that version.
  /// 属性 4：快照引用计数往返一致性
  /// **Validates: Requirements 2.2, 2.4, 3.2**
  /// **Feature: levels-version-refcount, Property 4: Reference count round-trip**
  #[test]
  fn prop_snapshot_lifecycle(ops in prop::collection::vec(snapshot_op_strategy(), 1..30)) {
    let refmap = Rc::new(RefCell::new(RefCountMap::new()));

    // Create levels with some tables
    // 创建带有一些表的层级
    let mut levels: Vec<Level<MockTable>> = (0..8).map(Level::new).collect();
    levels[0].add(MockTable::new(1));
    levels[0].add(MockTable::new(2));
    levels[1].add(MockTable::new(3));

    // Track live snapshots and expected counts per version
    // 追踪活跃快照和每个版本的预期计数
    let mut live_snapshots: Vec<Snapshot<MockTable>> = Vec::new();
    let mut expected_counts: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
    let mut current_ver: u64 = 0;

    for op in ops {
      match op {
        SnapshotOp::Create => {
          // Create snapshot at current version
          // 在当前版本创建快照
          let snapshot = Snapshot::new(current_ver, &levels, Rc::clone(&refmap));
          live_snapshots.push(snapshot);
          *expected_counts.entry(current_ver).or_insert(0) += 1;
          current_ver += 1;
        }
        SnapshotOp::Clone(idx) => {
          if !live_snapshots.is_empty() {
            let idx = idx % live_snapshots.len();
            let cloned = live_snapshots[idx].clone();
            let ver = cloned.ver();
            live_snapshots.push(cloned);
            *expected_counts.entry(ver).or_insert(0) += 1;
          }
        }
        SnapshotOp::Drop(idx) => {
          if !live_snapshots.is_empty() {
            let idx = idx % live_snapshots.len();
            let snapshot = live_snapshots.remove(idx);
            let ver = snapshot.ver();
            drop(snapshot);
            if let Some(count) = expected_counts.get_mut(&ver) {
              *count = count.saturating_sub(1);
              if *count == 0 {
                expected_counts.remove(&ver);
              }
            }
          }
        }
      }

      // Verify refcount matches expected after each operation
      // 每次操作后验证引用计数与预期匹配
      let refmap_borrow = refmap.borrow();
      for (&ver, &exp_count) in &expected_counts {
        let actual = refmap_borrow.get(ver);
        prop_assert_eq!(
          actual, exp_count,
          "refcount mismatch for ver {}: expected {}, got {}",
          ver, exp_count, actual
        );
      }
    }

    // Drop all remaining snapshots
    // 删除所有剩余快照
    drop(live_snapshots);

    // Verify all refcounts are zero
    // 验证所有引用计数为零
    let refmap_borrow = refmap.borrow();
    for ver in 0..current_ver {
      let count = refmap_borrow.get(ver);
      prop_assert_eq!(count, 0, "ver {} should have refcount 0 after all drops", ver);
    }
  }

  /// Property: Snapshot captures correct tables
  /// 属性：快照捕获正确的表
  #[test]
  fn prop_snapshot_captures_tables(
    l0_count in 0..5usize,
    l1_count in 0..5usize
  ) {
    let refmap = Rc::new(RefCell::new(RefCountMap::new()));

    // Create levels with tables
    // 创建带有表的层级
    let mut levels: Vec<Level<MockTable>> = (0..8).map(Level::new).collect();

    let mut next_id = 1u64;
    for _ in 0..l0_count {
      levels[0].add(MockTable::new(next_id));
      next_id += 1;
    }
    for _ in 0..l1_count {
      levels[1].add(MockTable::new(next_id));
      next_id += 1;
    }

    // Create snapshot
    // 创建快照
    let snapshot = Snapshot::new(0, &levels, Rc::clone(&refmap));

    // Verify snapshot has correct table counts
    // 验证快照有正确的表数量
    prop_assert_eq!(snapshot.level(0).len(), l0_count, "L0 table count mismatch");
    prop_assert_eq!(snapshot.level(1).len(), l1_count, "L1 table count mismatch");

    // Verify iter returns all tables
    // 验证 iter 返回所有表
    let total: usize = snapshot.iter().count();
    prop_assert_eq!(total, l0_count + l1_count, "total table count mismatch");

    // Verify version
    // 验证版本
    prop_assert_eq!(snapshot.ver(), 0, "version mismatch");
  }
}
