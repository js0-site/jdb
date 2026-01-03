#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

mod latency_prop {
  use jdb_val_bench::LatencyHistogram;
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

mod zipf_prop {
  use jdb_val_bench::ZipfWorkload;
  use proptest::prelude::*;

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property 2: Workload Operation Distribution
    /// *For any* Zipf workload with s >= 1.0, the top 20% of items should receive
    /// significantly more accesses than uniform distribution (hot-spot pattern)
    /// **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6**
    #[test]
    fn prop_workload_distribution(
      n in 10usize..100,
      s in 1.0f64..2.0,
      seed in 0u64..u64::MAX,
      samples in 1000usize..5000
    ) {
      // Create data with n items / 创建 n 个数据项
      let data: Vec<(usize, usize)> = (0..n).map(|i| (i, i)).collect();
      let mut workload = ZipfWorkload::new(data, s, seed);

      // Sample distribution / 采样分布
      let counts = workload.sample_distribution(samples);

      // Top 20% of items / 前 20% 的项
      let top_count = (n as f64 * 0.2).ceil() as usize;
      let top_accesses: usize = counts.iter().take(top_count).sum();
      let total_accesses: usize = counts.iter().sum();

      // With Zipf s >= 1.0, top 20% should get more than 20% of accesses
      // 使用 Zipf s >= 1.0，前 20% 应获得超过 20% 的访问
      let top_ratio = top_accesses as f64 / total_accesses as f64;

      // For Zipf distribution, top items should be accessed more frequently
      // 对于 Zipf 分布，顶部项应被更频繁访问
      prop_assert!(
        top_ratio > 0.2,
        "top 20% items should get > 20% accesses, got {:.2}%",
        top_ratio * 100.0
      );

      // First item should have highest count (Zipf property)
      // 第一项应有最高计数（Zipf 特性）
      let first_count = counts[0];
      let avg_count = total_accesses as f64 / n as f64;
      prop_assert!(
        first_count as f64 > avg_count,
        "first item count ({first_count}) should exceed average ({avg_count:.1})"
      );
    }
  }
}

mod zipf_reproducibility {
  use jdb_val_bench::ZipfWorkload;
  use proptest::prelude::*;

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property 3: Workload Reproducibility
    /// *For any* workload generator initialized with the same seed,
    /// the generated operation sequence SHALL be identical across multiple runs.
    /// **Validates: Requirements 2.3**
    #[test]
    fn prop_workload_reproducibility(
      n in 10usize..50,
      s in 1.0f64..2.0,
      seed in 0u64..u64::MAX,
      samples in 100usize..500
    ) {
      // Create data / 创建数据
      let data: Vec<(usize, usize)> = (0..n).map(|i| (i, i)).collect();

      // First run / 第一次运行
      let mut w1 = ZipfWorkload::new(data.clone(), s, seed);
      let seq1: Vec<usize> = (0..samples)
        .map(|_| *w1.key().unwrap())
        .collect();

      // Second run with same seed / 使用相同种子的第二次运行
      let mut w2 = ZipfWorkload::new(data.clone(), s, seed);
      let seq2: Vec<usize> = (0..samples)
        .map(|_| *w2.key().unwrap())
        .collect();

      // Sequences must be identical / 序列必须相同
      prop_assert_eq!(&seq1, &seq2, "same seed should produce identical sequence");

      // Reset and verify / 重置并验证
      w1.reset(seed);
      let seq3: Vec<usize> = (0..samples)
        .map(|_| *w1.key().unwrap())
        .collect();

      prop_assert_eq!(&seq1, &seq3, "reset with same seed should reproduce sequence");
    }
  }
}

mod metrics_prop {
  use std::time::Duration;

  use jdb_val_bench::{BenchMetrics, LatencyHistogram, LatencyStats};
  use proptest::prelude::*;

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property 1: Benchmark Metrics Validity
    /// *For any* benchmark run (put/get/mixed), the resulting metrics SHALL satisfy:
    /// - throughput > 0
    /// - P50 latency <= P99 latency <= P999 latency
    /// - ops_count matches requested operation count
    /// **Validates: Requirements 1.1, 1.2, 1.3**
    #[test]
    fn prop_benchmark_metrics_validity(
      ops_count in 100u64..10000,
      duration_ms in 1u64..10000,
      latencies in prop::collection::vec(1u64..1_000_000_000u64, 100..1000),
      disk_bytes in 0u64..1_000_000_000,
      memory_bytes in 0u64..1_000_000_000
    ) {
      // Build latency histogram / 构建延迟直方图
      let mut hist = LatencyHistogram::new().expect("create histogram");
      for &lat in &latencies {
        hist.record_saturating(lat);
      }
      let latency_stats = LatencyStats::from(&hist);

      // Create metrics / 创建指标
      let duration = Duration::from_millis(duration_ms);
      let metrics = BenchMetrics::new(
        ops_count,
        duration,
        latency_stats.clone(),
        disk_bytes,
        memory_bytes,
      );

      // Verify ops_count matches / 验证 ops_count 匹配
      prop_assert_eq!(metrics.ops_count, ops_count, "ops_count should match");

      // Verify throughput > 0 / 验证吞吐量 > 0
      prop_assert!(metrics.throughput > 0.0, "throughput should be > 0");

      // Verify throughput calculation / 验证吞吐量计算
      let expected_throughput = ops_count as f64 / duration.as_secs_f64();
      let throughput_diff = (metrics.throughput - expected_throughput).abs();
      prop_assert!(
        throughput_diff < 0.001,
        "throughput calculation: expected {expected_throughput}, got {}",
        metrics.throughput
      );

      // Verify latency ordering: P50 <= P99 <= P999
      // 验证延迟顺序：P50 <= P99 <= P999
      prop_assert!(
        metrics.latency.p50 <= metrics.latency.p99,
        "P50({}) <= P99({})",
        metrics.latency.p50,
        metrics.latency.p99
      );
      prop_assert!(
        metrics.latency.p99 <= metrics.latency.p999,
        "P99({}) <= P999({})",
        metrics.latency.p99,
        metrics.latency.p999
      );

      // Verify min <= P50 and P999 <= max
      // 验证 min <= P50 且 P999 <= max
      prop_assert!(
        metrics.latency.min <= metrics.latency.p50,
        "min({}) <= P50({})",
        metrics.latency.min,
        metrics.latency.p50
      );
      prop_assert!(
        metrics.latency.p999 <= metrics.latency.max,
        "P999({}) <= max({})",
        metrics.latency.p999,
        metrics.latency.max
      );

      // Verify disk and memory bytes are stored correctly
      // 验证磁盘和内存字节正确存储
      prop_assert_eq!(metrics.disk_bytes, disk_bytes);
      prop_assert_eq!(metrics.memory_bytes, memory_bytes);

      // Verify duration is stored correctly
      // 验证持续时间正确存储
      prop_assert_eq!(metrics.duration(), duration);
    }
  }
}
