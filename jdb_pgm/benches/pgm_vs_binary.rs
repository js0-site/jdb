//! Criterion benchmark comparing Pgm-Index vs binary search vs pgm_index crate
//! Criterion 基准测试：Pgm-Index vs 二分查找 vs pgm_index crate

use std::{
  collections::{BTreeMap, HashMap},
  fs::File,
  hint::black_box,
  io::Write,
  time::Instant,
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use jdb_pgm::Pgm;
use pgm_index as external_pgm;
use rand::{Rng, SeedableRng, rngs::StdRng};
use sonic_rs::{json, to_string_pretty};
#[cfg(feature = "jemalloc")]
use tikv_jemalloc_ctl::{epoch, stats};

// Set jemalloc as global allocator for accurate memory measurement
// 设置 jemalloc 为全局分配器以准确测量内存
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SAMPLE_SIZE: usize = 20;

/// Measure memory allocation using jemalloc
/// 使用 jemalloc 测量内存分配
#[cfg(feature = "jemalloc")]
fn measure_memory<F, T>(f: F) -> (T, usize)
where
  F: FnOnce() -> T,
{
  let _ = epoch::advance();
  let before = stats::allocated::read().unwrap_or(0);
  let result = f();
  let _ = epoch::advance();
  let after = stats::allocated::read().unwrap_or(0);
  (result, after.saturating_sub(before))
}

#[cfg(not(feature = "jemalloc"))]
fn measure_memory<F, T>(f: F) -> (T, usize)
where
  F: FnOnce() -> T,
{
  (f(), 0)
}

#[derive(serde::Serialize)]
struct BenchmarkResult {
  name: String,
  data_size: usize,
  epsilon: Option<usize>,
  algorithm: String,
  mean_ns: f64,
  std_dev_ns: f64,
  median_ns: f64,
  throughput: f64,
  memory_bytes: usize,
}

#[derive(serde::Serialize)]
struct BenchmarkConfig {
  query_count: usize,
  data_sizes: Vec<usize>,
  epsilon_values: Vec<usize>,
}

#[derive(serde::Serialize)]
struct BenchmarkData {
  config: BenchmarkConfig,
  results: Vec<BenchmarkResult>,
}

/// Calculate statistics from timing data
/// 从计时数据计算统计信息
fn calc_stats(times: &mut [f64]) -> (f64, f64, f64) {
  times.sort_by(|a, b| a.partial_cmp(b).unwrap());
  let mean = times.iter().sum::<f64>() / times.len() as f64;
  let variance = times.iter().map(|&t| (t - mean).powi(2)).sum::<f64>() / times.len() as f64;
  (mean, variance.sqrt(), times[times.len() / 2])
}

fn benchmark_binary_search(data: &[u64], queries: &[u64]) -> (f64, f64, f64) {
  let mut times: Vec<f64> = queries
    .iter()
    .map(|&q| {
      let start = Instant::now();
      let _ = data.binary_search(&q);
      start.elapsed().as_nanos() as f64
    })
    .collect();
  calc_stats(&mut times)
}

fn benchmark_btreemap(data: &[u64], queries: &[u64]) -> ((f64, f64, f64), usize) {
  let (map, mem) = measure_memory(|| {
    data
      .iter()
      .enumerate()
      .map(|(i, &v)| (v, i))
      .collect::<BTreeMap<_, _>>()
  });
  let mut times: Vec<f64> = queries
    .iter()
    .map(|&q| {
      let start = Instant::now();
      let _ = map.get(&q);
      start.elapsed().as_nanos() as f64
    })
    .collect();
  (calc_stats(&mut times), mem)
}

fn benchmark_hashmap(data: &[u64], queries: &[u64]) -> ((f64, f64, f64), usize) {
  let (map, mem) = measure_memory(|| {
    data
      .iter()
      .enumerate()
      .map(|(i, &v)| (v, i))
      .collect::<HashMap<_, _>>()
  });
  let mut times: Vec<f64> = queries
    .iter()
    .map(|&q| {
      let start = Instant::now();
      let _ = map.get(&q);
      start.elapsed().as_nanos() as f64
    })
    .collect();
  (calc_stats(&mut times), mem)
}

fn benchmark_pgm(data: &[u64], queries: &[u64], epsilon: usize) -> ((f64, f64, f64), usize) {
  let (pgm, mem) = measure_memory(|| Pgm::new(data, epsilon, false).unwrap());
  let mut times: Vec<f64> = queries
    .iter()
    .map(|&q| {
      let start = Instant::now();
      let (lo, hi) = pgm.predict_range(q);
      let _ = data[lo..hi.min(data.len())].binary_search(&q);
      start.elapsed().as_nanos() as f64
    })
    .collect();
  (calc_stats(&mut times), mem)
}

fn benchmark_external_pgm(
  data: &[u64],
  queries: &[u64],
  epsilon: usize,
) -> ((f64, f64, f64), usize) {
  let (index, mem) = measure_memory(|| external_pgm::PGMIndex::new(data.to_vec(), epsilon));
  let mut times: Vec<f64> = queries
    .iter()
    .map(|&q| {
      let start = Instant::now();
      let _ = index.get(q);
      start.elapsed().as_nanos() as f64
    })
    .collect();
  (calc_stats(&mut times), mem)
}

fn generate_benchmark_json() {
  println!("=== Generating benchmark JSON data ===\n");

  const QUERY_COUNT: usize = 1_500_000;
  const DATA_SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];
  const EPSILON_VALUES: [usize; 3] = [32, 64, 128];

  let mut results = Vec::new();
  let mut rng = StdRng::seed_from_u64(42);

  for &data_size in &DATA_SIZES {
    println!("Testing data size: {}", data_size);

    let data: Vec<u64> = (0..data_size as u64).collect();
    let queries: Vec<u64> = (0..QUERY_COUNT)
      .map(|_| rng.random_range(0..data_size as u64))
      .collect();

    // Binary search (no extra memory)
    println!("  Running binary search...");
    let times = benchmark_binary_search(&data, &queries);
    results.push(BenchmarkResult {
      name: format!("binary_search/{data_size}"),
      data_size,
      epsilon: None,
      algorithm: "binary_search".to_string(),
      mean_ns: times.0,
      std_dev_ns: times.1,
      median_ns: times.2,
      throughput: if times.0 > 0.0 { 1e9 / times.0 } else { 0.0 },
      memory_bytes: 0,
    });

    // BTreeMap
    println!("  Running BTreeMap...");
    let (times, mem) = benchmark_btreemap(&data, &queries);
    results.push(BenchmarkResult {
      name: format!("btreemap/{data_size}"),
      data_size,
      epsilon: None,
      algorithm: "btreemap".to_string(),
      mean_ns: times.0,
      std_dev_ns: times.1,
      median_ns: times.2,
      throughput: if times.0 > 0.0 { 1e9 / times.0 } else { 0.0 },
      memory_bytes: mem,
    });

    // HashMap
    println!("  Running HashMap...");
    let (times, mem) = benchmark_hashmap(&data, &queries);
    results.push(BenchmarkResult {
      name: format!("hashmap/{data_size}"),
      data_size,
      epsilon: None,
      algorithm: "hashmap".to_string(),
      mean_ns: times.0,
      std_dev_ns: times.1,
      median_ns: times.2,
      throughput: if times.0 > 0.0 { 1e9 / times.0 } else { 0.0 },
      memory_bytes: mem,
    });

    // Pgm with different epsilon values
    for &epsilon in &EPSILON_VALUES {
      println!("  Running jdb_pgm (epsilon = {epsilon})...");
      let (times, mem) = benchmark_pgm(&data, &queries, epsilon);
      results.push(BenchmarkResult {
        name: format!("jdb_pgm_eps_{epsilon}/{data_size}"),
        data_size,
        epsilon: Some(epsilon),
        algorithm: "jdb_pgm".to_string(),
        mean_ns: times.0,
        std_dev_ns: times.1,
        median_ns: times.2,
        throughput: if times.0 > 0.0 { 1e9 / times.0 } else { 0.0 },
        memory_bytes: mem,
      });

      println!("  Running external_pgm (epsilon = {epsilon})...");
      let (times, mem) = benchmark_external_pgm(&data, &queries, epsilon);
      results.push(BenchmarkResult {
        name: format!("external_pgm_eps_{epsilon}/{data_size}"),
        data_size,
        epsilon: Some(epsilon),
        algorithm: "external_pgm".to_string(),
        mean_ns: times.0,
        std_dev_ns: times.1,
        median_ns: times.2,
        throughput: if times.0 > 0.0 { 1e9 / times.0 } else { 0.0 },
        memory_bytes: mem,
      });
    }
  }

  // Sort by throughput descending
  results.sort_by(|a, b| {
    b.throughput
      .partial_cmp(&a.throughput)
      .unwrap_or(std::cmp::Ordering::Equal)
  });

  let data = BenchmarkData {
    config: BenchmarkConfig {
      query_count: QUERY_COUNT,
      data_sizes: DATA_SIZES.to_vec(),
      epsilon_values: EPSILON_VALUES.to_vec(),
    },
    results,
  };

  let json = to_string_pretty(&data).unwrap();
  File::create("bench.json")
    .unwrap()
    .write_all(json.as_bytes())
    .unwrap();
  println!("\nBenchmark data written to bench.json");
}

fn generate_accuracy_and_build_time_json() {
  println!("=== Generating accuracy and build time JSON data ===\n");

  let mut accuracy_results = Vec::new();
  let mut build_time_results = Vec::new();
  let mut rng = StdRng::seed_from_u64(42);

  for size in [10_000usize, 100_000, 1_000_000] {
    // Generate non-uniform random data
    // 生成非均匀随机数据
    let mut data: Vec<u64> = (0..size as u64)
      .map(|_| rng.random_range(0..(size as u64 * 10)))
      .collect();
    data.sort();
    data.dedup();

    while data.len() < size {
      data.push(rng.random_range(0..(size as u64 * 10)));
      data.sort();
      data.dedup();
    }
    data.truncate(size);

    println!("Data size: {}, unique elements: {}", size, data.len());

    // Generate random queries within data range
    // 在数据范围内生成随机查询
    let num_samples = 10000;
    let min_val = data[0];
    let max_val = data[data.len() - 1];
    let queries: Vec<u64> = (0..num_samples)
      .map(|_| rng.random_range(min_val..=max_val))
      .collect();

    for epsilon in [32, 64, 128] {
      // Linear interpolation error as baseline for both implementations
      // 线性插值误差作为两个实现的基准
      let range = max_val - min_val;
      let mut linear_max_error = 0usize;
      let mut linear_total_error = 0usize;

      for &query in &queries {
        let linear_pred = if range > 0 {
          ((query - min_val) as f64 / range as f64 * (data.len() - 1) as f64) as usize
        } else {
          0
        };
        let actual = match data.binary_search(&query) {
          Ok(pos) => pos,
          Err(pos) => pos.min(data.len() - 1),
        };
        let error = linear_pred.abs_diff(actual);
        linear_max_error = linear_max_error.max(error);
        linear_total_error += error;
      }
      let _linear_avg_error = linear_total_error as f64 / queries.len() as f64;

      // jdb_pgm
      println!("  Testing jdb_pgm (size={size}, epsilon={epsilon})...");
      let start = Instant::now();
      let jdb_pgm = Pgm::new(&data, epsilon, false).expect("build jdb pgm");
      let jdb_build_time = start.elapsed().as_nanos() as f64;

      build_time_results.push(json!({
        "data_size": size,
        "epsilon": epsilon,
        "algorithm": "jdb_pgm",
        "build_time_ns": jdb_build_time
      }));

      // Measure Pgm prediction error for each key in the dataset
      // 测量数据集中每个键的 Pgm 预测误差
      let mut jdb_max_error = 0usize;
      let mut jdb_total_error = 0usize;
      for (actual_pos, &key) in data.iter().enumerate() {
        let pred_pos = jdb_pgm.predict(key);
        let error = pred_pos.abs_diff(actual_pos);
        jdb_max_error = jdb_max_error.max(error);
        jdb_total_error += error;
      }
      let jdb_avg_error = jdb_total_error as f64 / data.len() as f64;
      println!("    Pgm error - Max: {jdb_max_error}, Avg: {jdb_avg_error:.2}");

      accuracy_results.push(json!({
        "data_size": size,
        "epsilon": epsilon,
        "algorithm": "jdb_pgm",
        "max_error": jdb_max_error,
        "avg_error": jdb_avg_error,
        "samples": data.len()
      }));

      // external pgm_index (forked version with public predict_index)
      // Fork 版本已公开 predict_index 方法，可以直接测量预测精度
      println!("  Testing pgm_index (size={size}, epsilon={epsilon})...");
      let start = Instant::now();
      let external_index = external_pgm::PGMIndex::new(data.clone(), epsilon);
      let external_build_time = start.elapsed().as_nanos() as f64;

      build_time_results.push(json!({
        "data_size": size,
        "epsilon": epsilon,
        "algorithm": "external_pgm",
        "build_time_ns": external_build_time
      }));

      // Measure actual prediction error for each key in the dataset
      // 测量数据集中每个键的实际预测误差
      let mut external_max_error = 0usize;
      let mut external_total_error = 0usize;

      for (actual_pos, &key) in data.iter().enumerate() {
        let pred_pos = external_index.predict_pos(key);
        let error = pred_pos.abs_diff(actual_pos);
        external_max_error = external_max_error.max(error);
        external_total_error += error;
      }
      let external_avg_error = external_total_error as f64 / data.len() as f64;
      println!("    Pgm error - Max: {external_max_error}, Avg: {external_avg_error:.2}");

      accuracy_results.push(json!({
        "data_size": size,
        "epsilon": epsilon,
        "algorithm": "external_pgm",
        "max_error": external_max_error,
        "avg_error": external_avg_error,
        "samples": data.len()
      }));
    }
  }

  let accuracy_json = json!({
    "config": {
      "sample_count": 10000,
      "data_sizes": [10000, 100000, 1000000],
      "epsilon_values": [32, 64, 128]
    },
    "results": accuracy_results
  });

  let build_time_json = json!({
    "config": {
      "data_sizes": [10000, 100000, 1000000],
      "epsilon_values": [32, 64, 128]
    },
    "results": build_time_results
  });

  File::create("accuracy.json")
    .unwrap()
    .write_all(to_string_pretty(&accuracy_json).unwrap().as_bytes())
    .unwrap();
  println!("\nAccuracy results written to accuracy.json");

  File::create("build_time.json")
    .unwrap()
    .write_all(to_string_pretty(&build_time_json).unwrap().as_bytes())
    .unwrap();
  println!("Build time results written to build_time.json");
}

fn main() {
  generate_benchmark_json();
  generate_accuracy_and_build_time_json();
  println!("\n=== Running Criterion benchmarks ===");
  benches();
}

fn bench_single_lookups(c: &mut Criterion) {
  let mut group = c.benchmark_group("single_lookups");
  group.sample_size(SAMPLE_SIZE);

  for size in [10_000usize, 100_000, 1_000_000] {
    let data: Vec<u64> = (0..size as u64).collect();
    let mut rng = StdRng::seed_from_u64(42);
    let queries: Vec<u64> = (0..1000)
      .map(|_| rng.random_range(0..size as u64))
      .collect();

    group.throughput(Throughput::Elements(queries.len() as u64));

    group.bench_with_input(
      BenchmarkId::new("binary_search", size),
      &(&data, &queries),
      |b, (data, queries)| {
        b.iter(|| {
          for &q in queries.iter() {
            let _ = black_box(data.binary_search(&q));
          }
        })
      },
    );

    for epsilon in [32, 64, 128] {
      let pgm = Pgm::new(&data, epsilon, false).unwrap();
      group.bench_with_input(
        BenchmarkId::new(format!("pgm_eps_{epsilon}"), size),
        &(&pgm, &data, &queries),
        |b, (pgm, data, queries): &(&Pgm<u64>, &Vec<u64>, &Vec<u64>)| {
          b.iter(|| {
            for &q in queries.iter() {
              let (lo, hi) = pgm.predict_range(q);
              let _ = black_box(data[lo..hi.min(data.len())].binary_search(&q));
            }
          })
        },
      );
    }
  }
  group.finish();
}

fn bench_batch_lookups(c: &mut Criterion) {
  let mut group = c.benchmark_group("batch_lookups");
  group.sample_size(SAMPLE_SIZE);

  let data: Vec<u64> = (0..1_000_000).collect();
  let mut rng = StdRng::seed_from_u64(42);

  for batch_size in [100, 1_000, 10_000] {
    let queries: Vec<u64> = (0..batch_size)
      .map(|_| rng.random_range(0..1_000_000u64))
      .collect();
    group.throughput(Throughput::Elements(batch_size as u64));

    group.bench_with_input(
      BenchmarkId::new("binary_search_seq", batch_size),
      &(&data, &queries),
      |b, (data, queries)| {
        b.iter(|| {
          let results: Vec<_> = queries.iter().map(|&q| data.binary_search(&q)).collect();
          black_box(results);
        })
      },
    );

    let pgm = Pgm::new(&data, 64, false).unwrap();
    group.bench_with_input(
      BenchmarkId::new("pgm_batch", batch_size),
      &(&pgm, &data, &queries),
      |b, (pgm, data, queries): &(&Pgm<u64>, &Vec<u64>, &Vec<u64>)| {
        b.iter(|| {
          let results: Vec<_> = queries
            .iter()
            .map(|&q| {
              let (lo, hi) = pgm.predict_range(q);
              data[lo..hi.min(data.len())].binary_search(&q)
            })
            .collect();
          black_box(results);
        })
      },
    );
  }
  group.finish();
}

fn bench_build_time(c: &mut Criterion) {
  let mut group = c.benchmark_group("build_time");
  group.sample_size(SAMPLE_SIZE);

  for size in [10_000usize, 100_000, 1_000_000] {
    let data: Vec<u64> = (0..size as u64).collect();
    group.throughput(Throughput::Elements(size as u64));

    for epsilon in [32, 64, 128] {
      group.bench_with_input(
        BenchmarkId::new(format!("pgm_eps_{epsilon}"), size),
        &(&data, epsilon),
        |b, (data, epsilon)| {
          b.iter(|| {
            let pgm = Pgm::new(data, *epsilon, false).unwrap();
            black_box(pgm);
          })
        },
      );
    }
  }
  group.finish();
}

fn bench_jdb_vs_external(c: &mut Criterion) {
  let mut group = c.benchmark_group("jdb_vs_external");
  group.sample_size(SAMPLE_SIZE);

  for size in [10_000usize, 100_000, 1_000_000] {
    let data: Vec<u64> = (0..size as u64).collect();
    let mut rng = StdRng::seed_from_u64(42);
    let queries: Vec<u64> = (0..1000)
      .map(|_| rng.random_range(0..size as u64))
      .collect();

    group.throughput(Throughput::Elements(queries.len() as u64));

    for epsilon in [32, 64, 128] {
      let jdb_pgm = Pgm::new(&data, epsilon, false).unwrap();
      group.bench_with_input(
        BenchmarkId::new(format!("jdb_eps_{epsilon}"), size),
        &(&jdb_pgm, &data, &queries),
        |b, (pgm, data, queries): &(&Pgm<u64>, &Vec<u64>, &Vec<u64>)| {
          b.iter(|| {
            for &q in queries.iter() {
              let (lo, hi) = pgm.predict_range(q);
              let _ = black_box(data[lo..hi.min(data.len())].binary_search(&q));
            }
          })
        },
      );

      let ext_index = external_pgm::PGMIndex::new(data.clone(), epsilon);
      group.bench_with_input(
        BenchmarkId::new(format!("ext_eps_{epsilon}"), size),
        &(&ext_index, &queries),
        |b, (index, queries): &(&external_pgm::PGMIndex<u64>, &Vec<u64>)| {
          b.iter(|| {
            for &q in queries.iter() {
              black_box(index.get(q));
            }
          })
        },
      );
    }
  }
  group.finish();
}

fn bench_accuracy(c: &mut Criterion) {
  let mut group = c.benchmark_group("accuracy");
  group.sample_size(SAMPLE_SIZE);

  for size in [10_000usize, 100_000, 1_000_000] {
    let mut rng = StdRng::seed_from_u64(42);
    let mut data: Vec<u64> = (0..size as u64)
      .map(|_| rng.random_range(0..(size as u64 * 10)))
      .collect();
    data.sort();
    data.dedup();
    while data.len() < size {
      data.push(rng.random_range(0..(size as u64 * 10)));
      data.sort();
      data.dedup();
    }
    data.truncate(size);

    let min_val = data[0];
    let max_val = data[data.len() - 1];
    let _range = max_val - min_val;
    let queries: Vec<u64> = (0..10000)
      .map(|_| rng.random_range(min_val..=max_val))
      .collect();

    group.throughput(Throughput::Elements(queries.len() as u64));

    for epsilon in [32, 64, 128] {
      let jdb_pgm = Pgm::new(&data, epsilon, false).unwrap();
      group.bench_with_input(
        BenchmarkId::new(format!("jdb_eps_{epsilon}_acc"), size),
        &(&data, &jdb_pgm),
        |b, (data, pgm)| {
          b.iter(|| {
            let mut max_err = 0usize;
            let mut total_err = 0usize;
            for (actual_pos, &key) in data.iter().enumerate() {
              let pred_pos = pgm.predict(key);
              let err = pred_pos.abs_diff(actual_pos);
              max_err = max_err.max(err);
              total_err += err;
            }
            black_box((max_err, total_err as f64 / data.len() as f64));
          })
        },
      );
    }
  }
  group.finish();
}

criterion_group!(
  benches,
  bench_single_lookups,
  bench_batch_lookups,
  bench_build_time,
  bench_jdb_vs_external,
  bench_accuracy
);
