// Engine benchmark with real corpus data
// 使用真实语料数据的引擎基准测试

use std::{
  fs,
  path::Path,
  time::{Duration, Instant},
};

use jdb_val_bench::{BenchEngine, EXPAND, KeyGen, MemBaseline, SEED, ZIPF_S, load_all};
use serde::{Deserialize, Serialize};
use sonic_rs::to_string_pretty;

const DATA_DIR: &str = "../jdb_bench_data/data";
const REPORT_DIR: &str = "report";
const BENCH_SECS: u64 = 3;
const WARMUP_SECS: u64 = 3;
const CHECK_INTERVAL: u64 = 100;
const MISS_RATE: f64 = 0.05;
const MB: f64 = 1024.0 * 1024.0;

#[derive(Serialize, Deserialize, Default)]
struct BenchResult {
  engine: String,
  duration_secs: u64,
  categories: Vec<(String, CategoryResult)>,
}

#[derive(Serialize, Deserialize)]
struct CategoryResult {
  write: OpResult,
  read: OpResult,
  write_amp: f64,
  disk_mb: f64,
  mem_mb: f64,
}

#[derive(Serialize, Deserialize)]
struct OpResult {
  ops: f64,
  mbs: f64,
}

impl OpResult {
  fn new(ops: u64, bytes: u64, elapsed: Duration) -> Self {
    let secs = elapsed.as_secs_f64();
    if secs > 0.0 {
      Self {
        ops: ops as f64 / secs,
        mbs: bytes as f64 / MB / secs,
      }
    } else {
      Self { ops: 0.0, mbs: 0.0 }
    }
  }
}

fn clean_dir(path: &Path) {
  if path.exists() {
    let _ = fs::remove_dir_all(path);
  }
}

type KvPair = (Vec<u8>, Vec<u8>);

fn bench_write<E: BenchEngine>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  items: &[KvPair],
  duration: Duration,
) -> (OpResult, u64) {
  let mut keygen = KeyGen::new(items.len());
  let mut ops = 0u64;
  let mut bytes = 0u64;
  let start = Instant::now();

  loop {
    let (key, val) = keygen.next_kv(items);
    rt.block_on(engine.put(&key, val)).expect("put failed");
    bytes += (key.len() + val.len()) as u64;
    ops += 1;
    if ops.is_multiple_of(CHECK_INTERVAL) && start.elapsed() >= duration {
      break;
    }
  }
  rt.block_on(engine.sync()).expect("sync failed");

  let result = OpResult::new(ops, bytes, start.elapsed());
  println!(
    "{label} write: {:>10.0} ops/s, {:>8.2} MB/s",
    result.ops, result.mbs
  );
  (result, bytes)
}

fn bench_read<E: BenchEngine>(
  rt: &compio::runtime::Runtime,
  engine: &mut E,
  label: &str,
  items: &[KvPair],
  duration: Duration,
) -> OpResult {
  let mut keygen = KeyGen::new(items.len());

  let warmup = Duration::from_secs(WARMUP_SECS);
  let warmup_start = Instant::now();
  while warmup_start.elapsed() < warmup {
    let key = keygen.next_key(items);
    let _ = rt.block_on(engine.get(&key));
  }

  engine.reset_stats();
  keygen.reset(SEED);
  let mut rng = fastrand::Rng::with_seed(SEED);

  let mut ops = 0u64;
  let mut bytes = 0u64;
  let mut hits = 0u64;
  let start = Instant::now();

  loop {
    let key = if rng.f64() < MISS_RATE {
      let (idx, _) = keygen.sample();
      let invalid_id = EXPAND + rng.u32(..1000);
      KeyGen::build_key(items[idx].0.as_ref(), invalid_id)
    } else {
      keygen.next_key(items)
    };

    if let Some(v) = rt.block_on(engine.get(&key)).expect("get failed") {
      bytes += v.as_ref().len() as u64;
      hits += 1;
    }
    ops += 1;
    if ops.is_multiple_of(CHECK_INTERVAL) && start.elapsed() >= duration {
      break;
    }
  }

  let result = OpResult::new(ops, bytes, start.elapsed());
  let hit_rate = if ops > 0 {
    hits as f64 / ops as f64 * 100.0
  } else {
    0.0
  };
  println!(
    "{label} read:  {:>10.0} ops/s, {:>8.2} MB/s (hit {hit_rate:.1}%)",
    result.ops, result.mbs
  );
  result
}

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
  println!("  Duration: {BENCH_SECS}s per test");
  println!();

  let large = to_kv(corpus.large.data());
  let medium = to_kv(corpus.medium.data());
  let small = to_kv(corpus.small.data());
  let data: [(&str, Vec<KvPair>); 3] = [("Large", large), ("Medium", medium), ("Small", small)];

  // Record memory baseline after data preload
  // 数据预加载后记录内存基准点
  let mem_baseline = MemBaseline::record();
  println!(
    "Memory baseline: {:.2} MB\n",
    mem_baseline.baseline() as f64 / MB
  );

  let _ = fs::create_dir_all(REPORT_DIR);
  let rt = compio::runtime::Runtime::new().expect("create runtime");
  let duration = Duration::from_secs(BENCH_SECS);

  #[cfg(feature = "jdb_val")]
  run_bench::<jdb_val_bench::JdbValAdapter>(
    &rt,
    "jdb_val",
    "/tmp/bench_jdb_val",
    &data,
    duration,
    &mem_baseline,
  );

  #[cfg(feature = "fjall")]
  run_bench::<jdb_val_bench::FjallAdapter>(
    &rt,
    "fjall",
    "/tmp/bench_fjall",
    &data,
    duration,
    &mem_baseline,
  );

  #[cfg(feature = "rocksdb")]
  run_bench::<jdb_val_bench::RocksDbAdapter>(
    &rt,
    "rocksdb",
    "/tmp/bench_rocksdb",
    &data,
    duration,
    &mem_baseline,
  );
}

trait EngineNew: BenchEngine + Sized {
  fn create(rt: &compio::runtime::Runtime, path: &Path) -> Self;
}

#[cfg(feature = "jdb_val")]
impl EngineNew for jdb_val_bench::JdbValAdapter {
  fn create(rt: &compio::runtime::Runtime, path: &Path) -> Self {
    rt.block_on(jdb_val_bench::JdbValAdapter::new(path))
      .expect("create jdb_val")
  }
}

#[cfg(feature = "fjall")]
impl EngineNew for jdb_val_bench::FjallAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_val_bench::FjallAdapter::new(path).expect("create fjall")
  }
}

#[cfg(feature = "rocksdb")]
impl EngineNew for jdb_val_bench::RocksDbAdapter {
  fn create(_rt: &compio::runtime::Runtime, path: &Path) -> Self {
    jdb_val_bench::RocksDbAdapter::new(path).expect("create rocksdb")
  }
}

fn bench_category<E: EngineNew>(
  rt: &compio::runtime::Runtime,
  path: &Path,
  label: &str,
  items: &[KvPair],
  duration: Duration,
  mem_baseline: &MemBaseline,
) -> CategoryResult {
  println!("[{label}]");
  clean_dir(path);
  let mut engine = E::create(rt, path);

  let (write, written_bytes) = bench_write(rt, &mut engine, "  ", items, duration);
  engine.flush_before_read();
  let read = bench_read(rt, &mut engine, "  ", items, duration);

  let disk = engine.disk_usage();
  let disk_mb = disk as f64 / MB;
  let write_amp = if written_bytes > 0 {
    disk as f64 / written_bytes as f64
  } else {
    0.0
  };
  let mem_mb = mem_baseline.db_mem() as f64 / MB;
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
  mem_baseline: &MemBaseline,
) {
  println!("=== {name} Benchmark ===\n");
  let path = Path::new(db_path);

  let categories: Vec<_> = data
    .iter()
    .map(|(label, items)| {
      let result = bench_category::<E>(rt, path, label, items, duration, mem_baseline);
      ((*label).to_string(), result)
    })
    .collect();

  let result = BenchResult {
    engine: name.to_string(),
    duration_secs: duration.as_secs(),
    categories,
  };
  let json = to_string_pretty(&result).expect("serialize json");
  let report_path = format!("{REPORT_DIR}/{name}.json");
  fs::write(&report_path, &json).expect("write report");
  println!("Report saved: {report_path}\n");
}
