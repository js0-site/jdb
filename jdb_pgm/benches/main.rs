//! Criterion benchmark comparing Pgm-Index vs binary search vs pgm_index crate
//! Criterion 基准测试：Pgm-Index vs 二分查找 vs pgm_index crate

mod bench_binary;
mod bench_btreemap;
mod bench_external_pgm;
mod bench_hashmap;
mod bench_jdb_pgm;

use std::{env, fs::File, hint::black_box, io::Write};

use bench_binary::BinarySearch;
use bench_btreemap::BTreeMapIndex;
use bench_external_pgm::ExternalPgm;
use bench_hashmap::HashMapIndex;
use bench_jdb_pgm::JdbPgm;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jdb_pgm::{
  Pgm,
  bench_common::{BenchResult, run_bench},
};
use pgm_index as external_pgm;
use rand::{Rng, SeedableRng, rngs::StdRng};
use sonic_rs::to_string_pretty;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SAMPLE_SIZE: usize = 20;
const QUERY_COUNT: usize = 1_500_000;
const DATA_SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];
const EPSILON_VALUES: [usize; 3] = [32, 64, 128];

/// Get benchmark config from env
/// 从环境变量获取评测配置
fn get_cfg() -> String {
  env::var("BENCH_CFG").unwrap_or_else(|_| "all".to_string())
}

/// Check if algorithm should run
/// 检查算法是否应该运行
fn should_run(algo: &str) -> bool {
  let cfg = get_cfg();
  match cfg.as_str() {
    "all" => true,
    "jdb_pgm" => algo == "jdb_pgm",
    "binary" => algo == "binary_search",
    "btreemap" => algo == "btreemap",
    "hashmap" => algo == "hashmap",
    "external" => algo == "external_pgm",
    _ => algo == "jdb_pgm",
  }
}

#[derive(serde::Serialize)]
struct BenchmarkResultJson {
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

impl From<BenchResult> for BenchmarkResultJson {
  fn from(r: BenchResult) -> Self {
    Self {
      name: r.name,
      data_size: r.data_size,
      epsilon: r.epsilon,
      algorithm: r.algorithm,
      mean_ns: r.mean_ns,
      std_dev_ns: r.std_dev_ns,
      median_ns: r.median_ns,
      throughput: r.throughput,
      memory_bytes: r.memory_bytes,
    }
  }
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
  results: Vec<BenchmarkResultJson>,
}

fn generate_benchmark_json() {
  println!("=== Generating benchmark JSON data ===\n");

  let mut results: Vec<BenchmarkResultJson> = Vec::new();
  let mut rng = StdRng::seed_from_u64(42);

  for &data_size in &DATA_SIZES {
    println!("Testing data size: {data_size}");

    let data: Vec<u64> = (0..data_size as u64).collect();
    let queries: Vec<u64> = (0..QUERY_COUNT)
      .map(|_| rng.random_range(0..data_size as u64))
      .collect();

    // Binary search
    if should_run("binary_search") {
      println!("  Running binary search...");
      results.push(run_bench::<BinarySearch>(&data, &queries, None).into());
    }

    // BTreeMap
    if should_run("btreemap") {
      println!("  Running BTreeMap...");
      results.push(run_bench::<BTreeMapIndex>(&data, &queries, None).into());
    }

    // HashMap
    if should_run("hashmap") {
      println!("  Running HashMap...");
      results.push(run_bench::<HashMapIndex>(&data, &queries, None).into());
    }

    // Pgm with different epsilon values
    for &epsilon in &EPSILON_VALUES {
      if should_run("jdb_pgm") {
        println!("  Running jdb_pgm (epsilon = {epsilon})...");
        results.push(run_bench::<JdbPgm>(&data, &queries, Some(epsilon)).into());
      }

      if should_run("external_pgm") {
        println!("  Running external_pgm (epsilon = {epsilon})...");
        results.push(run_bench::<ExternalPgm>(&data, &queries, Some(epsilon)).into());
      }
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

fn generate_accuracy_json() {
  println!("=== Generating accuracy and build time JSON data ===\n");

  let mut accuracy_results = Vec::new();
  let mut build_time_results = Vec::new();
  let mut rng = StdRng::seed_from_u64(42);

  for size in [10_000usize, 100_000, 1_000_000] {
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

    println!("Data size: {size}, unique elements: {}", data.len());

    for epsilon in [32, 64, 128] {
      // jdb_pgm
      if should_run("jdb_pgm") {
        println!("  Testing jdb_pgm (size={size}, epsilon={epsilon})...");
        let start = std::time::Instant::now();
        let jdb_pgm = Pgm::new(&data, epsilon, false).expect("build jdb pgm");
        let jdb_build_time = start.elapsed().as_nanos() as f64;

        build_time_results.push(sonic_rs::json!({
          "data_size": size,
          "epsilon": epsilon,
          "algorithm": "jdb_pgm",
          "build_time_ns": jdb_build_time
        }));

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

        accuracy_results.push(sonic_rs::json!({
          "data_size": size,
          "epsilon": epsilon,
          "algorithm": "jdb_pgm",
          "max_error": jdb_max_error,
          "avg_error": jdb_avg_error,
          "samples": data.len()
        }));
      }

      // external pgm_index
      if should_run("external_pgm") {
        println!("  Testing pgm_index (size={size}, epsilon={epsilon})...");
        let start = std::time::Instant::now();
        let external_index = external_pgm::PGMIndex::new(data.clone(), epsilon);
        let external_build_time = start.elapsed().as_nanos() as f64;

        build_time_results.push(sonic_rs::json!({
          "data_size": size,
          "epsilon": epsilon,
          "algorithm": "external_pgm",
          "build_time_ns": external_build_time
        }));

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

        accuracy_results.push(sonic_rs::json!({
          "data_size": size,
          "epsilon": epsilon,
          "algorithm": "external_pgm",
          "max_error": external_max_error,
          "avg_error": external_avg_error,
          "samples": data.len()
        }));
      }
    }
  }

  let accuracy_json = sonic_rs::json!({
    "config": {
      "sample_count": 10000,
      "data_sizes": [10000, 100000, 1000000],
      "epsilon_values": [32, 64, 128]
    },
    "results": accuracy_results
  });

  let build_time_json = sonic_rs::json!({
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

    if should_run("binary_search") {
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
    }

    for epsilon in [32, 64, 128] {
      if should_run("jdb_pgm") {
        let pgm = Pgm::new(&data, epsilon, false).unwrap();
        group.bench_with_input(
          BenchmarkId::new(format!("jdb_eps_{epsilon}"), size),
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

      if should_run("external_pgm") {
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

    if should_run("binary_search") {
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
    }

    if should_run("jdb_pgm") {
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
  }
  group.finish();
}

fn bench_build_time(c: &mut Criterion) {
  if !should_run("jdb_pgm") {
    return;
  }

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
  if !should_run("jdb_pgm") && !should_run("external_pgm") {
    return;
  }

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
      if should_run("jdb_pgm") {
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
      }

      if should_run("external_pgm") {
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
  }
  group.finish();
}

fn bench_accuracy(c: &mut Criterion) {
  if !should_run("jdb_pgm") {
    return;
  }

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

    let queries: Vec<u64> = {
      let min_val = data[0];
      let max_val = data[data.len() - 1];
      (0..10000)
        .map(|_| rng.random_range(min_val..=max_val))
        .collect()
    };

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

criterion_main!(benches);

/// Entry point for JSON generation (called via cargo run)
/// JSON 生成入口点（通过 cargo run 调用）
#[allow(dead_code)]
fn main_json() {
  generate_benchmark_json();
  generate_accuracy_json();
}
