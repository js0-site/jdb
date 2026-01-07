//! Property-based tests for RefCountMap
//! RefCountMap 属性测试

use jdb_level::RefCountMap;
use proptest::prelude::*;

/// Operation on RefCountMap
/// RefCountMap 操作
#[derive(Debug, Clone)]
enum Op {
  Inc(u64),
  Dec(u64),
  AddPending(u64, u64),
}

/// Generate random operations
/// 生成随机操作
fn op_strategy() -> impl Strategy<Value = Op> {
  prop_oneof![
    (0..10u64).prop_map(Op::Inc),
    (0..10u64).prop_map(Op::Dec),
    (0..10u64, 0..100u64).prop_map(|(ver, id)| Op::AddPending(ver, id)),
  ]
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// Property 4: Reference count round-trip consistency
  /// For any Snapshot, creating it increments refcount by 1, cloning it increments by 1,
  /// and dropping it decrements by 1. The refcount for a version SHALL equal the number
  /// of live Snapshot instances for that version.
  /// 属性 4：引用计数往返一致性
  /// **Validates: Requirements 2.2, 2.4, 3.2**
  /// **Feature: levels-version-refcount, Property 4: Reference count round-trip**
  #[test]
  fn prop_refcount_roundtrip(ops in prop::collection::vec(op_strategy(), 0..50)) {
    let mut map = RefCountMap::new();
    // Track expected counts per version
    // 追踪每个版本的预期计数
    let mut expected: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();

    for op in ops {
      match op {
        Op::Inc(ver) => {
          map.inc(ver);
          *expected.entry(ver).or_insert(0) += 1;
        }
        Op::Dec(ver) => {
          let reached_zero = map.dec(ver);
          if let Some(count) = expected.get_mut(&ver) {
            *count = count.saturating_sub(1);
            if *count == 0 {
              expected.remove(&ver);
              prop_assert!(reached_zero, "dec should return true when count reaches zero");
            } else {
              prop_assert!(!reached_zero, "dec should return false when count > 0");
            }
          } else {
            // Decrementing non-existent version should not panic
            // 递减不存在的版本不应 panic
            prop_assert!(!reached_zero);
          }
        }
        Op::AddPending(ver, id) => {
          map.add_pending(ver, id);
        }
      }
    }

    // Verify final state matches expected
    // 验证最终状态与预期匹配
    for (&ver, &exp_count) in &expected {
      let actual = map.get(ver);
      prop_assert_eq!(actual, exp_count, "count mismatch for ver {}", ver);
    }

    // Verify non-existent versions return 0
    // 验证不存在的版本返回 0
    for ver in 100..110 {
      if !expected.contains_key(&ver) {
        let actual = map.get(ver);
        prop_assert_eq!(actual, 0, "non-existent ver {} should return 0", ver);
      }
    }
  }

  /// Property: drain_safe returns correct IDs based on active refs
  /// drain_safe should only return IDs whose version has no active refs
  /// 属性：drain_safe 根据活跃引用返回正确的 ID
  /// **Feature: levels-version-refcount, Property 6: Deferred deletion**
  #[test]
  fn prop_drain_safe_correctness(
    inc_vers in prop::collection::vec(0..5u64, 0..10),
    pending in prop::collection::vec((0..10u64, 0..100u64), 0..20)
  ) {
    let mut map = RefCountMap::new();

    // Add refs
    // 添加引用
    for ver in &inc_vers {
      map.inc(*ver);
    }

    // Add pending deletions
    // 添加待删除项
    for (ver, id) in &pending {
      map.add_pending(*ver, *id);
    }

    // Get min active version
    // 获取最小活跃版本
    let min_active: Option<u64> = inc_vers.iter().copied().min();

    // Drain safe deletions
    // 排出安全删除项
    let safe = map.drain_safe();

    // Count how many times each ID should appear in safe (from versions < min_active)
    // 计算每个 ID 应该在 safe 中出现的次数（来自 < min_active 的版本）
    let mut expected_safe_count: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for (ver, id) in &pending {
      let should_be_safe = match min_active {
        Some(min) => *ver < min,
        None => true, // No active refs, all pending are safe
      };
      if should_be_safe {
        *expected_safe_count.entry(*id).or_insert(0) += 1;
      }
    }

    // Count actual occurrences in safe
    // 计算 safe 中的实际出现次数
    let mut actual_safe_count: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for id in &safe {
      *actual_safe_count.entry(*id).or_insert(0) += 1;
    }

    // Verify counts match
    // 验证计数匹配
    for (id, expected) in &expected_safe_count {
      let actual = actual_safe_count.get(id).copied().unwrap_or(0);
      prop_assert_eq!(actual, *expected, "ID {} count mismatch: expected {}, got {}", id, expected, actual);
    }

    // Verify no unexpected IDs in safe
    // 验证 safe 中没有意外的 ID
    for (id, actual) in &actual_safe_count {
      let expected = expected_safe_count.get(id).copied().unwrap_or(0);
      prop_assert_eq!(*actual, expected, "unexpected ID {} in safe: got {} occurrences", id, actual);
    }
  }

  /// Property: has_refs_before correctness
  /// 属性：has_refs_before 正确性
  #[test]
  fn prop_has_refs_before(
    inc_vers in prop::collection::vec(0..20u64, 0..10),
    check_ver in 0..30u64
  ) {
    let mut map = RefCountMap::new();

    for ver in &inc_vers {
      map.inc(*ver);
    }

    let has_refs = map.has_refs_before(check_ver);
    let expected = inc_vers.iter().any(|&v| v <= check_ver);

    prop_assert_eq!(has_refs, expected, "has_refs_before({}) mismatch", check_ver);
  }
}
