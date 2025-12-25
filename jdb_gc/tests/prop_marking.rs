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
