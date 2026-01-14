//! Unified benchmark runner for FSST implementations
//!
//! Features:
//! - bench_my: benchmark my implementation only
//! - bench_fsst_ref: benchmark reference implementation only
//! - bench_all: benchmark both (includes bench_my + bench_fsst_ref)

use std::{
  path::Path,
  time::{Duration, Instant},
};

use comfy_table::{ContentArrangement, Table};
use human_size::{Byte, Megabyte, SpecificSize};

// Include trait definition
include!("impl/trait.rs");

// Conditionally include implementations
#[cfg(feature = "bench_my")]
include!("impl/my.rs");

#[cfg(feature = "bench_fsst_ref")]
include!("impl/ref.rs");

const TXT_DIR: &str = "tests/txt";
const TXT_EXT: &str = "txt";
const TEST_SIZE_1MB: usize = 1024 * 1024;
const TEST_SIZE_2MB: usize = 2 * 1024 * 1024;
const ITERATIONS: usize = 10;

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

fn lines_to_buf_offsets(text: &str) -> (Vec<u8>, Vec<usize>) {
  let mut buf = Vec::new();
  let mut offsets = vec![0usize];

  for line in text.lines() {
    buf.extend_from_slice(line.as_bytes());
    offsets.push(buf.len());
  }
  (buf, offsets)
}

/// Benchmark result for a single implementation on a single test case
struct BenchResult {
  compressed_size: usize,
  duration: Duration,
  original_size: usize,
}

impl BenchResult {
  fn ratio(&self) -> f64 {
    self.compressed_size as f64 / self.original_size as f64 * 100.0
  }
}

/// Run benchmark for a single implementation
fn run_bench<T: FsstBench>(bench: &mut T, in_buf: &[u8], in_offsets: &[usize]) -> BenchResult {
  bench.reset(in_buf.len(), in_offsets.len());

  // Measure ratio (1 run)
  let compressed_size = bench.compress(in_buf, in_offsets);

  // Measure speed (multiple runs)
  let start = Instant::now();
  for _ in 0..ITERATIONS {
    bench.compress(in_buf, in_offsets);
  }
  let duration = start.elapsed();

  BenchResult {
    compressed_size,
    duration,
    original_size: in_buf.len(),
  }
}

/// Aggregate stats across all test cases
struct AggregateStats {
  total_original: usize,
  total_compressed: usize,
  total_processed: usize,
  total_duration: Duration,
}

impl AggregateStats {
  fn new() -> Self {
    Self {
      total_original: 0,
      total_compressed: 0,
      total_processed: 0,
      total_duration: Duration::ZERO,
    }
  }

  fn add(&mut self, result: &BenchResult) {
    self.total_original += result.original_size;
    self.total_compressed += result.compressed_size;
    self.total_processed += result.original_size * ITERATIONS;
    self.total_duration += result.duration;
  }

  fn avg_ratio(&self) -> f64 {
    self.total_compressed as f64 / self.total_original as f64 * 100.0
  }

  fn throughput_mbps(&self) -> f64 {
    (self.total_processed as f64 / 1024.0 / 1024.0) / self.total_duration.as_secs_f64()
  }
}

fn main() {
  #[cfg(not(any(feature = "bench_my", feature = "bench_fsst_ref")))]
  {
    eprintln!("No benchmark enabled. Use one of:");
    eprintln!("  cargo bench --features bench_my");
    eprintln!("  cargo bench --features bench_fsst_ref");
    eprintln!("  cargo bench --features bench_all");
    return;
  }

  let is_json = std::env::args().any(|arg| arg == "--json");

  #[cfg(any(feature = "bench_my", feature = "bench_fsst_ref"))]
  {
    let test_texts = load_test_texts();

    #[cfg(feature = "bench_my")]
    let mut my_bench = MyFsst::new(TEST_SIZE_2MB * 2, 100000);
    #[cfg(feature = "bench_fsst_ref")]
    let mut ref_bench = RefFsst::new();

    #[cfg(feature = "bench_my")]
    let mut my_stats = AggregateStats::new();
    #[cfg(feature = "bench_fsst_ref")]
    let mut ref_stats = AggregateStats::new();

    // Create table
    let mut table = Table::new();
    table
      .load_preset(comfy_table::presets::NOTHING)
      .set_content_arrangement(ContentArrangement::Dynamic);

    #[cfg(all(feature = "bench_my", feature = "bench_fsst_ref"))]
    table.set_header(vec![
      "文件",
      "大小",
      "我的压缩率",
      "参考压缩率",
      "我的吞吐(MB/s)",
      "参考吞吐(MB/s)",
      "加速",
      "压缩率倍数",
    ]);

    #[cfg(all(feature = "bench_my", not(feature = "bench_fsst_ref")))]
    table.set_header(vec!["文件", "大小", "我的压缩率", "我的吞吐(MB/s)"]);

    #[cfg(all(feature = "bench_fsst_ref", not(feature = "bench_my")))]
    table.set_header(vec!["文件", "大小", "参考压缩率", "参考吞吐(MB/s)"]);

    for (text, file_path) in &test_texts {
      for test_size in [TEST_SIZE_1MB, TEST_SIZE_2MB] {
        let repeat_num = test_size / text.len();
        let test_input = text.repeat(repeat_num.max(1));
        let (in_buf, in_offsets) = lines_to_buf_offsets(&test_input);
        let file_name = Path::new(file_path)
          .file_name()
          .and_then(|n| n.to_str())
          .unwrap_or(file_path);
        let file_label = format!("{} ({}MB)", file_name, test_size / (1024 * 1024));
        let size_str = format!(
          "{:.2}",
          SpecificSize::new(in_buf.len() as f64, Byte)
            .unwrap()
            .into::<Megabyte>()
        );

        #[cfg(feature = "bench_my")]
        let my_result = run_bench(&mut my_bench, &in_buf, &in_offsets);
        #[cfg(feature = "bench_fsst_ref")]
        let ref_result = run_bench(&mut ref_bench, &in_buf, &in_offsets);

        #[cfg(feature = "bench_my")]
        my_stats.add(&my_result);
        #[cfg(feature = "bench_fsst_ref")]
        ref_stats.add(&ref_result);

        // Add row to table
        #[cfg(all(feature = "bench_my", feature = "bench_fsst_ref"))]
        {
          let speedup = ref_result.duration.as_secs_f64() / my_result.duration.as_secs_f64();
          let my_throughput =
            (my_result.original_size as f64 * ITERATIONS as f64 / 1024.0 / 1024.0)
              / my_result.duration.as_secs_f64();
          let ref_throughput =
            (ref_result.original_size as f64 * ITERATIONS as f64 / 1024.0 / 1024.0)
              / ref_result.duration.as_secs_f64();
          let ratio_improvement = ref_result.ratio() / my_result.ratio();
          table.add_row(vec![
            file_label,
            size_str,
            format!("{:.2}%", my_result.ratio()),
            format!("{:.2}%", ref_result.ratio()),
            format!("{:.2}", my_throughput),
            format!("{:.2}", ref_throughput),
            format!("{:.2}x", speedup),
            format!("{:.3}x", ratio_improvement),
          ]);
        }
        #[cfg(all(feature = "bench_my", not(feature = "bench_fsst_ref")))]
        {
          let my_throughput =
            (my_result.original_size as f64 * ITERATIONS as f64 / 1024.0 / 1024.0)
              / my_result.duration.as_secs_f64();
          table.add_row(vec![
            file_label,
            size_str,
            format!("{:.2}%", my_result.ratio()),
            format!("{:.2}", my_throughput),
          ]);
        }
        #[cfg(all(feature = "bench_fsst_ref", not(feature = "bench_my")))]
        {
          let ref_throughput =
            (ref_result.original_size as f64 * ITERATIONS as f64 / 1024.0 / 1024.0)
              / ref_result.duration.as_secs_f64();
          table.add_row(vec![
            file_label,
            size_str,
            format!("{:.2}%", ref_result.ratio()),
            format!("{:.2}", ref_throughput),
          ]);
        }
      }
    }

    if !is_json {
      println!();
      println!("{}", table);
      println!();

      let mut summary = Table::new();
      summary
        .load_preset(comfy_table::presets::NOTHING)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["指标", "结果"]);

      let total_mb = {
        #[cfg(feature = "bench_my")]
        {
          my_stats.total_original as f64 / 1024.0 / 1024.0
        }
        #[cfg(all(not(feature = "bench_my"), feature = "bench_fsst_ref"))]
        {
          ref_stats.total_original as f64 / 1024.0 / 1024.0
        }
      };

      summary.add_row(vec!["总原始大小", &format!("{:.2} MB", total_mb)]);

      #[cfg(feature = "bench_my")]
      {
        summary.add_row(vec![
          "我的平均压缩率",
          &format!("{:.2}%", my_stats.avg_ratio()),
        ]);
        summary.add_row(vec![
          "我的总吞吐 (MB/s)",
          &format!("{:.2}", my_stats.throughput_mbps()),
        ]);
      }

      #[cfg(feature = "bench_fsst_ref")]
      {
        summary.add_row(vec![
          "参考平均压缩率",
          &format!("{:.2}%", ref_stats.avg_ratio()),
        ]);
        summary.add_row(vec![
          "参考总吞吐 (MB/s)",
          &format!("{:.2}", ref_stats.throughput_mbps()),
        ]);
      }

      #[cfg(all(feature = "bench_my", feature = "bench_fsst_ref"))]
      {
        summary.add_row(vec![
          "加速倍数 (吞吐)",
          &format!(
            "{:.2}x",
            my_stats.throughput_mbps() / ref_stats.throughput_mbps()
          ),
        ]);
        summary.add_row(vec![
          "压缩率提升 (Ref/My)",
          &format!("{:.3}x", ref_stats.avg_ratio() / my_stats.avg_ratio()),
        ]);
      }

      println!("\n总结:");
      println!("{}", summary);
    } else {
      print!("{{");
      let mut first = true;
      #[cfg(feature = "bench_my")]
      {
        print!(
          "\"my_ratio\": {:.4}, \"my_throughput\": {:.2}",
          my_stats.avg_ratio(),
          my_stats.throughput_mbps()
        );
        first = false;
      }
      #[cfg(feature = "bench_fsst_ref")]
      {
        if !first {
          print!(", ");
        }
        print!(
          "\"ref_ratio\": {:.4}, \"ref_throughput\": {:.2}",
          ref_stats.avg_ratio(),
          ref_stats.throughput_mbps()
        );
        first = false;
      }
      #[cfg(all(feature = "bench_my", feature = "bench_fsst_ref"))]
      {
        if !first {
          print!(", ");
        }
        print!(
          "\"speedup\": {:.4}, \"ratio_speedup\": {:.4}",
          my_stats.throughput_mbps() / ref_stats.throughput_mbps(),
          ref_stats.avg_ratio() / my_stats.avg_ratio()
        );
      }
      println!("}}");
    }
  }
}
