// Engine benchmark with real corpus data
// 使用真实语料数据的引擎基准测试

use std::{fs, path::Path, time::Instant};

use jdb_bench::{BenchEngine, load_all};
use serde::{Deserialize, Serialize};
use sonic_rs::to_string_pretty;

const DATA_DIR: &str = "data";
const REPORT_DIR: &str = "report";
const ZIPF_S: f64 = 1.2;
const SEED: u64 = 42;

#[derive(Serialize, Deserialize)]
struct BenchResult {
  engine: String,
  large: DataResult,
  medium: DataResult,
  small: DataResult,
  disk_bytes: u64,
  mem_bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct DataResult {
  ops: usize,
  elapsed_ms: u64,
  ops_per_sec: f64,
}

impl DataResult {
  fn new(ops: usize, elapsed_ms: u64) -> Self {
    let ops_per_sec = if elapsed_ms > 0 {
      ops as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
      0.0
    };
    Self {
      ops,
      elapsed_ms,
      ops_per_sec,
    }
  }
}

fn clean_dir(path: &Path) {
  if path.exists() {
    let _ = fs::remove_dir_all(path);
  }
}

/// Benchmark a batch of put operations / 批量 put 操作基准测试
fn bench_puts<E, F>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  iter: impl Iterator<Item = F>,
) -> DataResult
where
  E: BenchEngine,
  F: FnOnce() -> (Vec<u8>, Vec<u8>),
{
  let items: Vec<_> = iter.map(|f| f()).collect();
  let ops = items.len();
  let start = Instant::now();
  for (k, v) in &items {
    rt.block_on(engine.put(k, v)).unwrap();
  }
  rt.block_on(engine.sync()).unwrap();
  let elapsed = start.elapsed();
  let elapsed_ms = elapsed.as_millis() as u64;
  let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
  println!(
    "{label}: {:>6} ops, {:>10}, {:>12.0} ops/s",
    ops,
    format!("{:.2?}", elapsed),
    ops_per_sec
  );
  DataResult::new(ops, elapsed_ms)
}

fn main() {
  let corpus = load_all(Path::new(DATA_DIR), ZIPF_S, SEED).expect("load corpus");

  println!("Corpus loaded:");
  println!("  Large: {} items", corpus.large.len());
  println!("  Medium: {} items", corpus.medium.len());
  println!("  Small: {} items", corpus.small.len());
  println!();

  let large: Vec<_> = corpus.large.data().iter().collect();
  let medium: Vec<_> = corpus.medium.data().iter().collect();
  let small: Vec<_> = corpus.small.data().iter().collect();

  let _ = fs::create_dir_all(REPORT_DIR);
  let rt = compio::runtime::Runtime::new().unwrap();

  #[cfg(feature = "jdb")]
  run_bench::<jdb_bench::JdbSlabAdapter>(
    &rt,
    "jdb_slab",
    "/tmp/bench_jdb",
    &large,
    &medium,
    &small,
  );

  #[cfg(feature = "sled")]
  run_bench::<jdb_bench::SledAdapter>(&rt, "sled", "/tmp/bench_sled", &large, &medium, &small);

  #[cfg(feature = "fjall")]
  run_bench::<jdb_bench::FjallAdapter>(&rt, "fjall", "/tmp/bench_fjall", &large, &medium, &small);

  #[cfg(feature = "rocksdb")]
  run_bench::<jdb_bench::RocksDbAdapter>(
    &rt,
    "rocksdb",
    "/tmp/bench_rocksdb",
    &large,
    &medium,
    &small,
  );
}

trait EngineNew: BenchEngine + Sized {
  fn create(rt: &compio::runtime::Runtime, path: &Path) -> Self;
}

#[cfg(feature = "jdb")]
impl EngineNew for jdb_bench::JdbSlabAdapter {
  fn create(rt: &compio::runtime::Runtime, path: &Path) -> Self {
    rt.block_on(jdb_bench::JdbSlabAdapter::new(path)).unwrap()
  }
}

#[cfg(feature = "sled")]
impl EngineNew for jdb_bench::SledAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_bench::SledAdapter::new(path).unwrap()
  }
}

#[cfg(feature = "fjall")]
impl EngineNew for jdb_bench::FjallAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_bench::FjallAdapter::new(path).unwrap()
  }
}

#[cfg(feature = "rocksdb")]
impl EngineNew for jdb_bench::RocksDbAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_bench::RocksDbAdapter::new(path).unwrap()
  }
}

fn run_bench<E: EngineNew>(
  rt: &compio::runtime::Runtime,
  name: &str,
  db_path: &str,
  large: &[&(String, String)],
  medium: &[&(String, String)],
  small: &[&(String, u64)],
) {
  println!("=== {name} Benchmark ===\n");
  let path = Path::new(db_path);
  clean_dir(path);

  let mut engine = E::create(rt, path);

  // Large text / 大文本
  let large_result = bench_puts(
    rt,
    &mut engine,
    "Large ",
    large.iter().map(|(k, v)| {
      let kb = k.as_bytes().to_vec();
      let vb = v.as_bytes().to_vec();
      move || (kb, vb)
    }),
  );

  // Medium text / 中等文本
  let medium_result = bench_puts(
    rt,
    &mut engine,
    "Medium",
    medium.iter().map(|(k, v)| {
      let kb = k.as_bytes().to_vec();
      let vb = v.as_bytes().to_vec();
      move || (kb, vb)
    }),
  );

  // Small data / 小数据
  let small_result = bench_puts(
    rt,
    &mut engine,
    "Small ",
    small.iter().map(|(k, v)| {
      let kb = k.as_bytes().to_vec();
      let vb = v.to_le_bytes().to_vec();
      move || (kb, vb)
    }),
  );

  let disk = engine.disk_usage();
  let mem = engine.memory_usage();
  println!("\nTotal: disk: {disk} bytes, mem: {mem} bytes\n");

  // Save report / 保存报告
  let result = BenchResult {
    engine: name.to_string(),
    large: large_result,
    medium: medium_result,
    small: small_result,
    disk_bytes: disk,
    mem_bytes: mem,
  };
  let json = to_string_pretty(&result).unwrap();
  let report_path = format!("{REPORT_DIR}/{name}.json");
  fs::write(&report_path, &json).unwrap();
  println!("Report saved: {report_path}\n");

  clean_dir(path);
}
