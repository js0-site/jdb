// Engine benchmark tests / 引擎基准测试

use std::path::Path;

use aok::Void;
use humansize::{BINARY, format_size};
use jdb_bench::{BenchConfig, BenchMetrics, BenchRunner, WorkloadType};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Format nanoseconds to human readable / 格式化纳秒为可读形式
fn fmt_ns(ns: u64) -> String {
  if ns >= 1_000_000_000 {
    format!("{:.2} s", ns as f64 / 1_000_000_000.0)
  } else if ns >= 1_000_000 {
    format!("{:.2} ms", ns as f64 / 1_000_000.0)
  } else if ns >= 1_000 {
    format!("{:.2} µs", ns as f64 / 1_000.0)
  } else {
    format!("{ns} ns")
  }
}

/// Print benchmark result / 打印基准测试结果
fn print_result(name: &str, metrics: &BenchMetrics) {
  info!("=== {name} ===");
  info!("  Ops: {}", metrics.ops_count);
  info!("  Duration: {:.2?}", metrics.duration());
  info!("  Throughput: {:.2} ops/sec", metrics.throughput);
  info!("  Latency P50: {}", fmt_ns(metrics.latency.p50));
  info!("  Latency P99: {}", fmt_ns(metrics.latency.p99));
  info!("  Latency P999: {}", fmt_ns(metrics.latency.p999));
  info!("  Disk: {}", format_size(metrics.disk_bytes, BINARY));
  info!("  Memory: {}", format_size(metrics.memory_bytes, BINARY));
  info!("");
}

/// Clean test directory / 清理测试目录
fn clean_dir(path: &Path) {
  if path.exists() {
    let _ = std::fs::remove_dir_all(path);
  }
}

#[cfg(feature = "jdb")]
#[compio::test]
async fn bench_jdb_slab() -> Void {
  use jdb_bench::JdbSlabAdapter;

  let path = Path::new("/tmp/bench_jdb_slab");
  clean_dir(path);

  let mut engine = JdbSlabAdapter::new(path).await?;
  let config = BenchConfig::new(
    vec![4096], // 4KB values
    vec![1000], // 1K ops
    vec![WorkloadType::Sequential],
  )
  .warmup(10);

  let mut runner = BenchRunner::with_seed(config, 42);
  let metrics = runner
    .run_single(&mut engine, WorkloadType::Sequential, 4096, 1000)
    .await?;
  print_result("jdb_slab", &metrics);

  clean_dir(path);
  Ok(())
}

#[cfg(feature = "fjall")]
#[compio::test]
async fn bench_fjall() -> Void {
  use jdb_bench::FjallAdapter;

  let path = Path::new("/tmp/bench_fjall");
  clean_dir(path);

  let mut engine = FjallAdapter::new(path)?;
  let config = BenchConfig::new(vec![4096], vec![1000], vec![WorkloadType::Sequential]).warmup(10);

  let mut runner = BenchRunner::with_seed(config, 42);
  let metrics = runner
    .run_single(&mut engine, WorkloadType::Sequential, 4096, 1000)
    .await?;
  print_result("fjall", &metrics);

  clean_dir(path);
  Ok(())
}

#[cfg(feature = "rocksdb")]
#[compio::test]
async fn bench_rocksdb() -> Void {
  use jdb_bench::RocksDbAdapter;

  let path = Path::new("/tmp/bench_rocksdb");
  clean_dir(path);

  let mut engine = RocksDbAdapter::new(path)?;
  let config = BenchConfig::new(vec![4096], vec![1000], vec![WorkloadType::Sequential]).warmup(10);

  let mut runner = BenchRunner::with_seed(config, 42);
  let metrics = runner
    .run_single(&mut engine, WorkloadType::Sequential, 4096, 1000)
    .await?;
  print_result("rocksdb", &metrics);

  clean_dir(path);
  Ok(())
}
