// Common benchmark utilities / 通用评测工具
// LruBench trait and shared data structures

use std::{
  fs::File,
  io::{Read, Seek, SeekFrom, Write},
  sync::OnceLock,
  time::Instant,
};

use serde::Serialize;

/// LRU cache benchmark trait / LRU 缓存评测 trait
pub trait LruBench {
  /// Library name / 库名称
  fn name(&self) -> &'static str;

  /// Set key-value pair / 设置键值对
  fn set(&mut self, key: &[u8], val: &[u8]);

  /// Get value, returns true if hit / 获取值，命中返回 true
  fn get(&mut self, key: &[u8]) -> bool;
}

/// Single benchmark result / 单次评测结果
#[derive(Serialize, Clone)]
pub struct BenchResult {
  pub lib: String,
  pub hit_rate: f64,
  pub ops_per_second: f64,
  pub effective_ops: f64,
  pub memory_kb: f64,
}

/// Category result (Large/Medium/Small) / 分类结果
#[derive(Serialize)]
pub struct CategoryResult {
  pub name: String,
  pub capacity_mb: f64,
  pub items: usize,
  pub results: Vec<BenchResult>,
}

/// Full benchmark output / 完整评测输出
#[derive(Serialize)]
pub struct BenchOutput {
  pub miss_latency_ns: u64,
  pub miss_latency_method: String,
  pub categories: Vec<CategoryResult>,
}

/// Capacity configuration / 容量配置
pub const LARGE_CAP: u64 = 64 * 1024 * 1024;
pub const MEDIUM_CAP: u64 = 16 * 1024 * 1024;
pub const SMALL_CAP: u64 = 1024 * 1024;

/// Benchmark loops / 评测循环次数
pub const LOOPS: usize = 3;

/// Operations per loop / 每循环操作数
pub const OPS_PER_LOOP: usize = 100_000;

/// Data directory / 数据目录
pub const DATA_DIR: &str = "../jdb_bench_data/data";

/// JSON output path / JSON 输出路径
pub const JSON_PATH: &str = "bench.json";

/// Global miss latency cache / 全局 miss 延迟缓存
static MISS_LATENCY: OnceLock<u64> = OnceLock::new();

/// Estimate NVMe read latency using tempfile / 使用临时文件估算 NVMe 读取延迟
/// Returns latency in nanoseconds / 返回纳秒级延迟
pub fn estimate_miss_latency() -> u64 {
  *MISS_LATENCY.get_or_init(|| {
    const BLOCK_SIZE: usize = 4096;
    const ITERATIONS: usize = 100;
    const FILE_SIZE: u64 = 16 * 1024 * 1024; // 16MB

    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("bench_io");

    // Create file with random data / 创建随机数据文件
    {
      let mut file = File::create(&path).expect("create file");
      let mut rng = fastrand::Rng::with_seed(42);
      let mut buf = vec![0u8; BLOCK_SIZE];
      let blocks = (FILE_SIZE as usize) / BLOCK_SIZE;
      for _ in 0..blocks {
        rng.fill(&mut buf);
        file.write_all(&buf).expect("write");
      }
      file.sync_all().expect("sync");
    }

    // Open file once, measure read latency / 打开文件一次，测量读取延迟
    let mut file = File::open(&path).expect("open file");
    let mut buf = vec![0u8; BLOCK_SIZE];
    let mut rng = fastrand::Rng::with_seed(123);
    let max_offset = FILE_SIZE - BLOCK_SIZE as u64;

    // Warmup / 预热
    for _ in 0..10 {
      let offset = rng.u64(0..max_offset);
      file.seek(SeekFrom::Start(offset)).expect("seek");
      file.read_exact(&mut buf).expect("read");
    }

    // Measure / 测量
    let start = Instant::now();
    for _ in 0..ITERATIONS {
      let offset = rng.u64(0..max_offset);
      file.seek(SeekFrom::Start(offset)).expect("seek");
      file.read_exact(&mut buf).expect("read");
    }
    let elapsed = start.elapsed();

    elapsed.as_nanos() as u64 / ITERATIONS as u64
  })
}

/// Miss latency estimation method description / miss 延迟估算方法描述
pub fn miss_latency_method() -> &'static str {
  "NVMe 4KB random read via tempfile (16MB file, 100 iterations, excludes open/close)"
}

/// Calculate effective OPS considering miss latency / 计算考虑 miss 延迟的有效 OPS
/// Formula: effective_ops = 1 / (hit_time + miss_rate * miss_latency)
#[inline]
pub fn calc_effective_ops(ops_per_second: f64, hit_rate: f64, miss_latency_ns: u64) -> f64 {
  let hit_time_ns = 1e9 / ops_per_second;
  let miss_rate = 1.0 - hit_rate;
  let avg_time_ns = hit_time_ns + miss_rate * miss_latency_ns as f64;
  1e9 / avg_time_ns
}
