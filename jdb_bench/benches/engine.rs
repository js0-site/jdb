// Engine benchmark with real corpus data
// 使用真实语料数据的引擎基准测试

use std::{
  fs,
  path::Path,
  time::{Duration, Instant},
};

use jdb_val_bench::{BenchEngine, KeyGen, SEED, load_all};
use serde::{Deserialize, Serialize};
use sonic_rs::to_string_pretty;

const DATA_DIR: &str = "data";
const REPORT_DIR: &str = "report";

/// Benchmark duration in seconds / 基准测试持续时间（秒）
const BENCH_SECS: u64 = 3;

/// Warmup duration in seconds / 预热时间（秒）
const WARMUP_SECS: u64 = 3;

/// Check interval / 检查间隔
const CHECK_INTERVAL: u64 = 100;

#[derive(Serialize, Deserialize, Default)]
struct BenchResult {
  engine: String,
  duration_secs: u64,
  categories: Vec<(String, CategoryResult)>,
}

#[derive(Serialize, Deserialize)]
struct CategoryResult {
  write: DataResult,
  read: DataResult,
  write_amp: f64,
  disk_mb: f64,
  mem_mb: f64,
}

#[derive(Serialize, Deserialize)]
struct DataResult {
  ops_per_sec: f64,
  mb_per_sec: f64,
}

impl DataResult {
  fn new(ops: u64, bytes: u64, elapsed: Duration) -> Self {
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
    let mb_per_sec = if secs > 0.0 {
      (bytes as f64 / 1024.0 / 1024.0) / secs
    } else {
      0.0
    };
    Self {
      ops_per_sec,
      mb_per_sec,
    }
  }
}

fn clean_dir(path: &Path) {
  if path.exists() {
    let _ = fs::remove_dir_all(path);
  }
}

/// Key-value pair type / 键值对类型
type KvPair = (Vec<u8>, Vec<u8>);

/// Benchmark write / 写入基准测试
fn bench_write<E: BenchEngine>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  items: &[KvPair],
  duration: Duration,
) -> (DataResult, u64) {
  let mut keygen = KeyGen::new(items.len());
  let mut ops = 0u64;
  let mut bytes = 0u64;
  let start = Instant::now();

  loop {
    let (key, val) = keygen.next_kv(items);
    rt.block_on(engine.put(&key, val)).unwrap();
    bytes += (key.len() + val.len()) as u64;
    ops += 1;
    if ops.is_multiple_of(CHECK_INTERVAL) && start.elapsed() >= duration {
      break;
    }
  }
  rt.block_on(engine.sync()).unwrap();

  let result = DataResult::new(ops, bytes, start.elapsed());
  println!(
    "{label} write: {:>10.0} ops/s, {:>8.2} MB/s",
    result.ops_per_sec, result.mb_per_sec
  );
  (result, bytes)
}

/// Benchmark read / 读取基准测试
fn bench_read<E: BenchEngine>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  items: &[KvPair],
  duration: Duration,
) -> DataResult {
  // Warmup with same seed / 使用相同种子预热
  let mut keygen = KeyGen::new(items.len());
  let warmup = Duration::from_secs(WARMUP_SECS);
  let warmup_start = Instant::now();
  while warmup_start.elapsed() < warmup {
    let key = keygen.next_key(items);
    let _ = rt.block_on(engine.get(&key));
  }

  // Reset stats and keygen / 重置统计和 keygen
  engine.reset_stats();
  keygen.reset(SEED);

  // Actual benchmark / 实际测试
  let mut ops = 0u64;
  let mut bytes = 0u64;
  let start = Instant::now();

  loop {
    let key = keygen.next_key(items);
    if let Some(v) = rt.block_on(engine.get(&key)).unwrap() {
      bytes += v.as_ref().len() as u64;
    }
    ops += 1;
    if ops.is_multiple_of(CHECK_INTERVAL) && start.elapsed() >= duration {
      break;
    }
  }

  let result = DataResult::new(ops, bytes, start.elapsed());
  println!(
    "{label} read:  {:>10.0} ops/s, {:>8.2} MB/s",
    result.ops_per_sec, result.mb_per_sec
  );
  result
}

/// Convert corpus to key-value bytes / 转换语料为键值字节
fn to_kv(data: &[(String, Vec<u8>)]) -> Vec<KvPair> {
  data
    .iter()
    .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
    .collect()
}

fn main() {
  let corpus = load_all(Path::new(DATA_DIR), jdb_val_bench::ZIPF_S, SEED).expect("load corpus");

  println!("Corpus loaded:");
  println!("  Large: {} items", corpus.large.len());
  println!("  Medium: {} items", corpus.medium.len());
  println!("  Small: {} items", corpus.small.len());
  println!("  Duration: {BENCH_SECS}s per test");
  println!();

  let large = to_kv(corpus.large.data());
  let medium = to_kv(corpus.medium.data());
  let small = to_kv(corpus.small.data());
  let data: Vec<(&str, Vec<KvPair>)> = vec![("Large", large), ("Medium", medium), ("Small", small)];

  let _ = fs::create_dir_all(REPORT_DIR);
  let rt = compio::runtime::Runtime::new().unwrap();
  let duration = Duration::from_secs(BENCH_SECS);

  #[cfg(feature = "jdb_val")]
  run_bench::<jdb_val_bench::JdbValAdapter>(&rt, "jdb_val", "/tmp/bench_jdb_val", &data, duration);

  #[cfg(feature = "fjall")]
  run_bench::<jdb_val_bench::FjallAdapter>(&rt, "fjall", "/tmp/bench_fjall", &data, duration);

  #[cfg(feature = "rocksdb")]
  run_bench::<jdb_val_bench::RocksDbAdapter>(&rt, "rocksdb", "/tmp/bench_rocksdb", &data, duration);
}

trait EngineNew: BenchEngine + Sized {
  fn create(rt: &compio::runtime::Runtime, path: &Path) -> Self;
}

#[cfg(feature = "jdb_val")]
impl EngineNew for jdb_val_bench::JdbValAdapter {
  fn create(rt: &compio::runtime::Runtime, path: &Path) -> Self {
    rt.block_on(jdb_val_bench::JdbValAdapter::new(path)).unwrap()
  }
}

#[cfg(feature = "fjall")]
impl EngineNew for jdb_val_bench::FjallAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_val_bench::FjallAdapter::new(path).unwrap()
  }
}

#[cfg(feature = "rocksdb")]
impl EngineNew for jdb_val_bench::RocksDbAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_val_bench::RocksDbAdapter::new(path).unwrap()
  }
}

/// Run category benchmark / 运行类别基准测试
fn bench_category<E: EngineNew>(
  rt: &compio::runtime::Runtime,
  path: &Path,
  label: &str,
  items: &[KvPair],
  duration: Duration,
) -> CategoryResult {
  println!("[{label}]");
  clean_dir(path);
  let mut engine = E::create(rt, path);

  let (write, written_bytes) = bench_write(rt, &mut engine, "  ", items, duration);
  let read = bench_read(rt, &mut engine, "  ", items, duration);

  let disk = engine.disk_usage();
  let disk_mb = disk as f64 / 1024.0 / 1024.0;
  let write_amp = if written_bytes > 0 {
    disk as f64 / written_bytes as f64
  } else {
    0.0
  };
  let mem_mb = engine.memory_usage() as f64 / 1024.0 / 1024.0;
  println!("   disk: {disk_mb:>8.2} MB ({write_amp:.2}x amp), mem: {mem_mb:>8.2} MB");
  println!();

  drop(engine);
  clean_dir(path);

  CategoryResult {
    write,
    read,
    write_amp,
    disk_mb,
    mem_mb,
  }
}

fn run_bench<E: EngineNew>(
  rt: &compio::runtime::Runtime,
  name: &str,
  db_path: &str,
  data: &[(&str, Vec<KvPair>)],
  duration: Duration,
) {
  println!("=== {name} Benchmark ===\n");
  let path = Path::new(db_path);

  let mut categories = Vec::new();
  for (label, items) in data {
    let result = bench_category::<E>(rt, path, label, items, duration);
    categories.push((label.to_string(), result));
  }

  let result = BenchResult {
    engine: name.to_string(),
    duration_secs: duration.as_secs(),
    categories,
  };
  let json = to_string_pretty(&result).unwrap();
  let report_path = format!("{REPORT_DIR}/{name}.json");
  fs::write(&report_path, &json).unwrap();
  println!("Report saved: {report_path}\n");
}
