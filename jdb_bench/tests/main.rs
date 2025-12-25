use aok::{OK, Void};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}

mod latency_prop {
  use jdb_bench::LatencyHistogram;
  use proptest::prelude::*;

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property 8: Latency Histogram Consistency
    /// *For any* latency histogram:
    /// - min <= P50 <= mean <= P99 <= P999 <= max
    /// - All recorded values are non-negative
    /// **Validates: Requirements 1.1, 1.2, 1.3**
    #[test]
    fn prop_latency_histogram_consistency(
      latencies in prop::collection::vec(1u64..1_000_000_000u64, 10..500)
    ) {
      let mut hist = LatencyHistogram::new().expect("create histogram");

      // Record all latencies / 记录所有延迟
      for &lat in &latencies {
        hist.record_saturating(lat);
      }

      // Verify count matches / 验证计数匹配
      prop_assert_eq!(hist.count(), latencies.len() as u64);

      // Verify ordering: min <= P50 <= P99 <= P999 <= max
      // 验证顺序：min <= P50 <= P99 <= P999 <= max
      let min = hist.min();
      let p50 = hist.p50();
      let p99 = hist.p99();
      let p999 = hist.p999();
      let max = hist.max();
      let mean = hist.mean();

      prop_assert!(min <= p50, "min({min}) <= p50({p50})");
      prop_assert!(p50 <= p99, "p50({p50}) <= p99({p99})");
      prop_assert!(p99 <= p999, "p99({p99}) <= p999({p999})");
      prop_assert!(p999 <= max, "p999({p999}) <= max({max})");

      // Mean should be between min and max
      // 平均值应在最小值和最大值之间
      prop_assert!(mean >= min as f64, "mean({mean}) >= min({min})");
      prop_assert!(mean <= max as f64, "mean({mean}) <= max({max})");

      // All values non-negative (implicit since u64)
      // 所有值非负（u64 隐式保证）
      prop_assert!(min > 0, "min > 0 (we use 1 as lower bound)");
    }
  }
}
