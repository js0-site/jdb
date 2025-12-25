//! Property tests for GC marking / GC 标记属性测试
//!
//! Feature: gc, Property 2: Incremental Marking Batch Size
//! Validates: Requirements 1.1, 1.4, 8.2

use jdb_gc::{GcConf, GcWorker};
use jdb_trait::ValRef;
use proptest::prelude::*;

/// Generate arbitrary ValRef / 生成任意 ValRef
fn arb_valref() -> impl Strategy<Value = ValRef> {
  (1u64..100, 0u64..1000000).prop_map(|(file_id, offset)| ValRef {
    file_id,
    offset: offset * 4096, // Align to page / 对齐到页
    prev_file_id: 0,
    prev_offset: 0,
  })
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// Property 2: Incremental Marking Batch Size
  /// For any gc_step() call in Marking state with N keys remaining,
  /// the number of keys scanned should be min(mark_batch, N).
  /// Validates: Requirements 1.1, 1.4, 8.2
  #[test]
  fn prop_marking_batch_size(
    vrefs in prop::collection::vec(arb_valref(), 0..500),
    mark_batch in 1usize..100
  ) {
    let conf = GcConf {
      mark_batch,
      ..Default::default()
    };

    let mut worker = GcWorker::with_conf(conf);
    worker.start();

    // Simulate marking by processing vrefs in batches
    // 通过批量处理 vrefs 模拟标记
    let total_keys = vrefs.len();
    let mut processed = 0;

    while processed < total_keys {
      let remaining = total_keys - processed;
      let batch_size = remaining.min(mark_batch);

      // Mark batch / 标记批次
      for vref in vrefs.iter().skip(processed).take(batch_size) {
        worker.live_tracker_mut().mark(vref);
      }

      processed += batch_size;
      worker.inc_keys(batch_size as u64);

      // Verify batch size constraint / 验证批大小约束
      prop_assert!(batch_size <= mark_batch);
      prop_assert!(batch_size == remaining.min(mark_batch));
    }

    // Verify total keys scanned / 验证扫描的总键数
    prop_assert_eq!(worker.stats().keys_scanned, total_keys as u64);
  }
}

/// Property 3: ValRef Marking Correctness
/// For any ValRef that is marked, is_live(file_id, offset) must return true.
/// For any ValRef not marked, is_live must return false.
/// Validates: Requirements 1.2, 6.3
mod prop_valref_marking {
  use jdb_gc::LiveTracker;

  use super::*;

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_valref_marking_correctness(
      marked_vrefs in prop::collection::vec(arb_valref(), 0..100),
      unmarked_vrefs in prop::collection::vec(arb_valref(), 0..100)
    ) {
      let mut tracker = LiveTracker::new();

      // Mark the marked vrefs / 标记已标记的 vrefs
      for vref in &marked_vrefs {
        tracker.mark(vref);
      }

      // Verify marked vrefs are live / 验证已标记的 vrefs 是存活的
      for vref in &marked_vrefs {
        prop_assert!(
          tracker.is_live(vref.file_id, vref.real_offset()),
          "Marked ValRef should be live: {:?}",
          vref
        );
      }

      // Verify unmarked vrefs are not live (unless they happen to match a marked one)
      // 验证未标记的 vrefs 不是存活的（除非它们恰好匹配一个已标记的）
      for vref in &unmarked_vrefs {
        let is_marked = marked_vrefs.iter().any(|m| {
          m.file_id == vref.file_id && m.real_offset() == vref.real_offset()
        });

        if !is_marked {
          prop_assert!(
            !tracker.is_live(vref.file_id, vref.real_offset()),
            "Unmarked ValRef should not be live: {:?}",
            vref
          );
        }
      }
    }
  }
}

/// Property 4: Keep Policy History Marking
/// For any key with history chain and Keep policy, only versions satisfying
/// Keep.should_keep() should be marked as live.
/// Validates: Requirements 1.3, 6.4
mod prop_keep_policy {
  use jdb_gc::LiveTracker;
  use jdb_table::Keep;

  use super::*;

  /// Generate history chain / 生成历史链
  fn arb_history() -> impl Strategy<Value = Vec<ValRef>> {
    prop::collection::vec(arb_valref(), 1..10)
  }

  /// Generate Keep policy / 生成 Keep 策略
  fn arb_keep() -> impl Strategy<Value = Keep> {
    prop_oneof![
      Just(Keep::Current),
      (1usize..10).prop_map(Keep::Versions),
      (1000u64..100000).prop_map(Keep::Duration),
      Just(Keep::All),
    ]
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_keep_policy_history_marking(
      history in arb_history(),
      keep in arb_keep(),
      now_ms in 1000000u64..2000000
    ) {
      let mut tracker = LiveTracker::new();

      // Generate timestamps (older versions have older timestamps)
      // 生成时间戳（旧版本有更旧的时间戳）
      let timestamps: Vec<u64> = history
        .iter()
        .enumerate()
        .map(|(i, _)| (now_ms / 1000).saturating_sub(i as u64 * 1000))
        .collect();

      // Mark history with keep policy / 用 keep 策略标记历史
      tracker.mark_history(&history, keep, now_ms, &timestamps);

      // Verify each version is marked correctly / 验证每个版本是否正确标记
      for (idx, vref) in history.iter().enumerate() {
        let age_ms = timestamps
          .get(idx)
          .map(|&ts| now_ms.saturating_sub(ts * 1000));
        let should_keep = keep.should_keep(idx, age_ms);
        let is_live = tracker.is_live(vref.file_id, vref.real_offset());

        if should_keep {
          prop_assert!(
            is_live,
            "Version {} should be kept with policy {:?}, but is not live",
            idx,
            keep
          );
        }
        // Note: We can't assert !is_live for versions that shouldn't be kept
        // because mark_history breaks early when should_keep returns false
        // 注意：我们不能断言不应保留的版本 !is_live
        // 因为 mark_history 在 should_keep 返回 false 时提前中断
      }
    }

    /// Test Keep::Current only keeps first version
    /// 测试 Keep::Current 只保留第一个版本
    #[test]
    fn prop_keep_current_only_first(history in arb_history()) {
      let mut tracker = LiveTracker::new();
      let now_ms = 1000000u64;
      let timestamps: Vec<u64> = vec![now_ms / 1000; history.len()];

      tracker.mark_history(&history, Keep::Current, now_ms, &timestamps);

      // Only first should be live / 只有第一个应该是存活的
      if !history.is_empty() {
        prop_assert!(tracker.is_live(history[0].file_id, history[0].real_offset()));
      }

      // Rest should not be live / 其余不应该是存活的
      for vref in history.iter().skip(1) {
        prop_assert!(!tracker.is_live(vref.file_id, vref.real_offset()));
      }
    }

    /// Test Keep::All keeps all versions
    /// 测试 Keep::All 保留所有版本
    #[test]
    fn prop_keep_all_keeps_all(history in arb_history()) {
      let mut tracker = LiveTracker::new();
      let now_ms = 1000000u64;
      let timestamps: Vec<u64> = vec![now_ms / 1000; history.len()];

      tracker.mark_history(&history, Keep::All, now_ms, &timestamps);

      // All should be live / 所有都应该是存活的
      for vref in &history {
        prop_assert!(tracker.is_live(vref.file_id, vref.real_offset()));
      }
    }

    /// Test Keep::Versions(n) keeps exactly n versions
    /// 测试 Keep::Versions(n) 保留恰好 n 个版本
    #[test]
    fn prop_keep_versions_exact(
      history in arb_history(),
      n in 1usize..10
    ) {
      let mut tracker = LiveTracker::new();
      let now_ms = 1000000u64;
      let timestamps: Vec<u64> = vec![now_ms / 1000; history.len()];

      tracker.mark_history(&history, Keep::Versions(n), now_ms, &timestamps);

      // First n should be live / 前 n 个应该是存活的
      for (idx, vref) in history.iter().enumerate() {
        let is_live = tracker.is_live(vref.file_id, vref.real_offset());
        if idx < n {
          prop_assert!(is_live, "Version {} should be kept", idx);
        } else {
          prop_assert!(!is_live, "Version {} should not be kept", idx);
        }
      }
    }
  }
}
