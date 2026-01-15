//! Unified benchmark runner for FSST implementations

use std::{hint::black_box, path::Path, time::Duration};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

mod r#impl;

#[cfg(feature = "bench_jdb_fsst")]
use r#impl::jdb_fsst::JdbFsst;
#[cfg(feature = "bench_fsst")]
use r#impl::fsst::Fsst;
use r#impl::r#trait::FsstBench;

const TXT_DIR: &str = "tests/txt";
const TXT_EXT: &str = "txt";
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
      .filter(|e| e.path().extension().is_some_and(|ext| ext == TXT_EXT))
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

fn bench_train_encode(c: &mut Criterion) {
  fastrand::seed(42);
  let test_texts = load_test_texts();

  let mut group = c.benchmark_group("fsst");
  group.warm_up_time(Duration::from_millis(100)); // 减少预热时间
  group.measurement_time(Duration::from_millis(200)); // 设置测量时间

  for (text, file_path) in &test_texts {
    {
      let test_size = TEST_SIZE_1MB;
      let repeat_num = test_size / text.len();
      let test_input = text.repeat(repeat_num.max(1));
      let lines: Vec<&[u8]> = test_input.lines().map(|l| l.as_bytes()).collect();

      // Extract language from path (en/zh)
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
        test_size / (1024 * 1024)
      );

      #[cfg(feature = "bench_jdb_fsst")]
      {
        let mut bench = JdbFsst::new();
        bench.prepare(&lines);
        let enc_id = format!("{}_enc", JdbFsst::NAME);
        let dec_id = format!("{}_dec", JdbFsst::NAME);
        group.bench_with_input(BenchmarkId::new(&enc_id, &label), &lines, |b, _| {
          b.iter(|| black_box(bench.train_and_encode()));
        });
        group.bench_function(BenchmarkId::new(&dec_id, &label), |b| {
          b.iter(|| {
            bench.decompress_all();
            black_box(())
          });
        });
      }

      #[cfg(feature = "bench_fsst")]
      {
        let mut bench = Fsst::new();
        bench.prepare(&lines);
        let enc_id = format!("{}_enc", Fsst::NAME);
        let dec_id = format!("{}_dec", Fsst::NAME);
        group.bench_with_input(BenchmarkId::new(&enc_id, &label), &lines, |b, _| {
          b.iter(|| black_box(bench.train_and_encode()));
        });
        group.bench_function(BenchmarkId::new(&dec_id, &label), |b| {
          b.iter(|| {
            bench.decompress_all();
            black_box(())
          });
        });
      }
    }
  }

  group.finish();
}

criterion_group! {
  name = benches;
  config = Criterion::default().sample_size(10);
  targets = bench_train_encode
}
criterion_main!(benches);
