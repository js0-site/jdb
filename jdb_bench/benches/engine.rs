// Engine benchmark with real corpus data
// 使用真实语料数据的引擎基准测试

use std::{
  fs,
  path::Path,
  time::{Duration, Instant},
};

use jdb_val_bench::{BenchEngine, load_all};
use serde::{Deserialize, Serialize};
use sonic_rs::to_string_pretty;

const DATA_DIR: &str = "data";
const REPORT_DIR: &str = "report";
const ZIPF_S: f64 = 1.2;
const SEED: u64 = 42;

/// Benchmark duration in seconds / 基准测试持续时间（秒）
const BENCH_DURATION_SECS: u64 = 3;

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
  /// Write amplification / 写放大率
  write_amp: f64,
  /// Disk usage in MB / 磁盘使用量 MB
  disk_mb: f64,
  mem_mb: f64,
}

#[derive(Serialize, Deserialize)]
struct DataResult {
  /// ops per second / 每秒操作数
  ops_per_sec: f64,
  /// MB per second / 每秒 MB
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

/// Check interval / 检查间隔
const CHECK_INTERVAL: u64 = 100;

/// Key expansion factor for unique keys / 唯一键扩容因子
///
/// Joint distribution: Zipf(item) × Uniform(0..EXPAND)
/// 联合分布：Zipf(item) × Uniform(0..EXPAND)
///
/// With s=1.2, top 10% items get ~90% access.
/// s=1.2 时，前 10% 的 item 获得约 90% 的访问。
///
/// For ~90% read hit rate in Large scenario (slowest, ~160K ops):
/// 为了在 Large 场景（最慢，约 160K ops）达到约 90% 读命中率：
///   - Hot items = 6987 * 10% ≈ 700
///   - Need: 160K >= 700 * EXPAND * 0.9
///   - EXPAND <= 160K / 630 ≈ 250
const EXPAND: u32 = 250;

/// Benchmark write for duration / 持续写入基准测试
/// Returns (DataResult, total_bytes_written, written_key_ids) / 返回 (结果, 总写入字节数, 写入的键索引)
fn bench_write<E: BenchEngine>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  items: &[(Vec<u8>, Vec<u8>)],
  duration: Duration,
) -> (DataResult, u64, Vec<(usize, u32)>) {
  let mut ops = 0u64;
  let mut bytes = 0u64;
  let mut key_ids = Vec::new();
  let mut rng = fastrand::Rng::with_seed(SEED);
  let start = Instant::now();

  loop {
    // Joint distribution: Zipf for item, uniform for id
    // 联合分布：Zipf 选 item，均匀选 id
    // P(item=i, id=j) = P_zipf(i) / EXPAND
    // Virtual key space = items.len() * EXPAND ≈ 1.7M unique keys
    // 虚拟键空间 = items.len() * EXPAND ≈ 170 万唯一键
    let idx = rng.usize(..items.len());
    let id = rng.u32(..EXPAND);
    let (k, v) = &items[idx];
    let mut key = k.clone();
    key.extend_from_slice(&id.to_le_bytes());
    rt.block_on(engine.put(&key, v)).unwrap();
    bytes += (key.len() + v.len()) as u64;
    // Only store (idx, id), reconstruct key when reading
    // 只存 (idx, id)，读取时重新构造 key
    key_ids.push((idx, id));
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
  (result, bytes, key_ids)
}

/// Benchmark read for duration / 持续读取基准测试
fn bench_read<E: BenchEngine>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  items: &[(Vec<u8>, Vec<u8>)],
  key_ids: &[(usize, u32)],
  duration: Duration,
) -> DataResult {
  let mut ops = 0u64;
  let mut bytes = 0u64;
  let mut idx = 0;
  let start = Instant::now();

  loop {
    let (item_idx, id) = key_ids[idx % key_ids.len()];
    // Reconstruct key from (idx, id) / 从 (idx, id) 重新构造 key
    let mut key = items[item_idx].0.clone();
    key.extend_from_slice(&id.to_le_bytes());
    if let Some(v) = rt.block_on(engine.get(&key)).unwrap() {
      bytes += v.len() as u64;
    }
    ops += 1;
    idx += 1;
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

/// Key-value pair type / 键值对类型
type KvPair = (Vec<u8>, Vec<u8>);

/// Convert corpus to key-value bytes / 转换语料为键值字节
fn to_kv(data: &[(String, Vec<u8>)]) -> Vec<KvPair> {
  data
    .iter()
    .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
    .collect()
}

fn main() {
  let corpus = load_all(Path::new(DATA_DIR), ZIPF_S, SEED).expect("load corpus");

  println!("Corpus loaded:");
  println!("  Large: {} items", corpus.large.len());
  println!("  Medium: {} items", corpus.medium.len());
  println!("  Small: {} items", corpus.small.len());
  println!("  Duration: {BENCH_DURATION_SECS}s per test");
  println!();

  let large = to_kv(corpus.large.data());
  let medium = to_kv(corpus.medium.data());
  let small = to_kv(corpus.small.data());
  let data: Vec<(&str, Vec<KvPair>)> = vec![("Large", large), ("Medium", medium), ("Small", small)];

  let _ = fs::create_dir_all(REPORT_DIR);
  let rt = compio::runtime::Runtime::new().unwrap();
  let duration = Duration::from_secs(BENCH_DURATION_SECS);

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
    rt.block_on(jdb_val_bench::JdbValAdapter::new(path))
      .unwrap()
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
  items: &[(Vec<u8>, Vec<u8>)],
  duration: Duration,
) -> CategoryResult {
  println!("[{label}]");
  clean_dir(path);
  let mut engine = E::create(rt, path);

  let (write, written_bytes, key_ids) = bench_write(rt, &mut engine, "  ", items, duration);
  let read = bench_read(rt, &mut engine, "  ", items, &key_ids, duration);

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

  // Save report / 保存报告
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
