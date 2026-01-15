//! Unified benchmark runner for FSST implementations
//! FSST 实现的统一基准测试

use std::{hint::black_box, path::Path, time::Duration};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

mod r#impl;

#[cfg(feature = "bench_jdb_fsst")]
use r#impl::jdb_fsst::JdbFsst;
#[cfg(feature = "bench_fsst")]
use r#impl::fsst::Fsst;
use r#impl::r#trait::FsstBench;

const TXT_DIR: &str = "tests/txt";
const TEST_SIZE_1MB: usize = 1024 * 1024;

fn load_test_texts() -> Vec<(String, String)> {
  let txt_dir = Path::new(TXT_DIR);
  let mut texts = Vec::new();
  for lang in ["en", "zh"] {
    let lang_dir = txt_dir.join(lang);
    if !lang_dir.exists() {
      continue;
    }
    let mut files: Vec<_> = std::fs::read_dir(&lang_dir)
      .unwrap()
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
      .collect();
    files.sort_by_key(|e| e.path());
    for entry in files {
      let path = entry.path();
      if let Ok(content) = std::fs::read_to_string(&path) {
        texts.push((content, path.to_string_lossy().into_owned()));
      }
    }
  }
  texts
}

/// Run benchmarks for a single FsstBench implementation
/// 对单个 FsstBench 实现运行基准测试
fn bench_impl<T: FsstBench + Default>(
  group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
  lines: &[&[u8]],
  label: &str,
) {
  let mut bench = T::default();
  bench.prepare(lines);
  bench.train_and_encode();

  let name = T::NAME;

  group.bench_function(BenchmarkId::new(format!("{name}_enc"), label), |b| {
    b.iter(|| black_box(bench.train_and_encode()));
  });

  group.bench_function(BenchmarkId::new(format!("{name}_dec"), label), |b| {
    b.iter(|| {
      bench.decode_all();
      black_box(())
    });
  });

  let num_items = bench.num_items();
  if num_items > 0 {
    group.bench_function(BenchmarkId::new(format!("{name}_random_dec"), label), |b| {
      b.iter(|| {
        for i in 0..num_items {
          bench.random_decode(i);
        }
        black_box(())
      });
    });
  }
}

fn bench_train_encode(c: &mut Criterion) {
  fastrand::seed(42);
  let test_texts = load_test_texts();

  let mut group = c.benchmark_group("fsst");
  group.warm_up_time(Duration::from_millis(100));
  group.measurement_time(Duration::from_millis(200));

  for (text, file_path) in &test_texts {
    let repeat_num = TEST_SIZE_1MB / text.len();
    let test_input = text.repeat(repeat_num.max(1));
    let lines: Vec<&[u8]> = test_input.lines().map(|l| l.as_bytes()).collect();

    let lang = if file_path.contains("/en/") {
      "en"
    } else if file_path.contains("/zh/") {
      "zh"
    } else {
      "xx"
    };
    let file_name = Path::new(file_path)
      .file_name()
      .and_then(|n| n.to_str())
      .unwrap_or(file_path);
    let label = format!(
      "{}_{}_{}MB",
      lang,
      file_name.replace('.', "_"),
      TEST_SIZE_1MB / (1024 * 1024)
    );

    #[cfg(feature = "bench_jdb_fsst")]
    bench_impl::<JdbFsst>(&mut group, &lines, &label);

    #[cfg(feature = "bench_fsst")]
    bench_impl::<Fsst>(&mut group, &lines, &label);
  }

  group.finish();
}

criterion_group! {
  name = benches;
  config = Criterion::default().sample_size(10);
  targets = bench_train_encode
}
criterion_main!(benches);
