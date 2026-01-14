//! Unified benchmark runner for FSST implementations
//!
//! Features:
//! - default: benchmark my implementation only
//! - bench_fsst_ref: benchmark reference implementation only
//! - bench_all: benchmark both and compare

use human_size::{Byte, Megabyte, SpecificSize};
use std::path::Path;
use std::time::{Duration, Instant};

// ============================================================================
// Trait Definition
// ============================================================================

/// A trait for FSST compression implementations to benchmark
pub trait FsstBench {
    /// Name of this implementation
    const NAME: &'static str;

    /// Compress data and return compressed size
    fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize;

    /// Reset internal buffers for next run
    fn reset(&mut self, buf_size: usize, offsets_len: usize);
}

// ============================================================================
// My Implementation
// ============================================================================

#[cfg(not(feature = "bench_fsst_ref"))]
mod my_impl {
    use super::FsstBench;
    use jdb_fsst::encode;

    pub struct MyFsst {
        output_buf: Vec<u8>,
        offset_buf: Vec<usize>,
    }

    impl MyFsst {
        pub fn new(buf_size: usize, offsets_len: usize) -> Self {
            Self {
                output_buf: vec![0; buf_size],
                offset_buf: vec![0; offsets_len],
            }
        }
    }

    impl FsstBench for MyFsst {
        const NAME: &'static str = "My";

        fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize {
            let _ = encode(
                in_buf,
                in_offsets,
                &mut self.output_buf,
                &mut self.offset_buf,
            );
            *self.offset_buf.last().unwrap_or(&0)
        }

        fn reset(&mut self, buf_size: usize, offsets_len: usize) {
            self.output_buf.resize(buf_size, 0);
            self.offset_buf.resize(offsets_len, 0);
        }
    }
}

// ============================================================================
// Reference Implementation
// ============================================================================

#[cfg(any(feature = "bench_all", feature = "bench_fsst_ref"))]
mod ref_impl {
    use super::FsstBench;

    #[allow(dead_code)]
    #[allow(unused_imports)]
    mod fsst_ref {
        include!("../tests/fsst_ref.rs");
    }

    pub struct RefFsst;

    impl RefFsst {
        pub fn new() -> Self {
            Self
        }
    }

    impl FsstBench for RefFsst {
        const NAME: &'static str = "Ref";

        fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize {
            let compressed = fsst_ref::compress(in_buf, in_offsets);
            compressed.len()
        }

        fn reset(&mut self, _buf_size: usize, _offsets_len: usize) {
            // No-op, ref implementation allocates internally
        }
    }
}

// ============================================================================
// Benchmark Infrastructure
// ============================================================================

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
fn run_bench<T: FsstBench>(
    bench: &mut T,
    in_buf: &[u8],
    in_offsets: &[usize],
) -> BenchResult {
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

// ============================================================================
// Main
// ============================================================================

fn main() {
    let test_texts = load_test_texts();

    // Prepare implementations based on features
    #[cfg(not(feature = "bench_fsst_ref"))]
    let mut my_bench = my_impl::MyFsst::new(TEST_SIZE_2MB * 2, 100000);
    #[cfg(any(feature = "bench_all", feature = "bench_fsst_ref"))]
    let mut ref_bench = ref_impl::RefFsst::new();

    // Aggregate stats
    #[cfg(not(feature = "bench_fsst_ref"))]
    let mut my_stats = AggregateStats::new();
    #[cfg(any(feature = "bench_all", feature = "bench_fsst_ref"))]
    let mut ref_stats = AggregateStats::new();

    // Print header
    #[cfg(feature = "bench_all")]
    println!(
        "{:<40} | {:<15} | {:<12} | {:<12} | {:<10}",
        "File", "Size", "My Ratio", "Ref Ratio", "Speedup"
    );
    #[cfg(feature = "bench_fsst_ref")]
    println!("{:<40} | {:<15} | {:<12}", "File", "Size", "Ref Ratio");
    #[cfg(not(any(feature = "bench_all", feature = "bench_fsst_ref")))]
    println!("{:<40} | {:<15} | {:<12}", "File", "Size", "My Ratio");

    println!("{}", "-".repeat(100));

    for (text, file_path) in &test_texts {
        for test_size in [TEST_SIZE_1MB, TEST_SIZE_2MB] {
            let repeat_num = test_size / text.len();
            let test_input = text.repeat(repeat_num.max(1));
            let (in_buf, in_offsets) = lines_to_buf_offsets(&test_input);
            let file_label = format!("{} ({}MB)", file_path, test_size / (1024 * 1024));
            let size_str = format!(
                "{:.2}",
                SpecificSize::new(in_buf.len() as f64, Byte)
                    .unwrap()
                    .into::<Megabyte>()
            );

            #[cfg(not(feature = "bench_fsst_ref"))]
            let my_result = run_bench(&mut my_bench, &in_buf, &in_offsets);
            #[cfg(any(feature = "bench_all", feature = "bench_fsst_ref"))]
            let ref_result = run_bench(&mut ref_bench, &in_buf, &in_offsets);

            // Add to aggregates
            #[cfg(not(feature = "bench_fsst_ref"))]
            my_stats.add(&my_result);
            #[cfg(any(feature = "bench_all", feature = "bench_fsst_ref"))]
            ref_stats.add(&ref_result);

            // Print row
            #[cfg(feature = "bench_all")]
            {
                let speedup = ref_result.duration.as_secs_f64() / my_result.duration.as_secs_f64();
                println!(
                    "{:<40} | {:<15} | {:.2}%       | {:.2}%       | {:.2}x",
                    file_label,
                    size_str,
                    my_result.ratio(),
                    ref_result.ratio(),
                    speedup
                );
            }
            #[cfg(feature = "bench_fsst_ref")]
            println!(
                "{:<40} | {:<15} | {:.2}%",
                file_label,
                size_str,
                ref_result.ratio()
            );
            #[cfg(not(any(feature = "bench_all", feature = "bench_fsst_ref")))]
            println!(
                "{:<40} | {:<15} | {:.2}%",
                file_label,
                size_str,
                my_result.ratio()
            );
        }
    }

    println!("{}", "-".repeat(100));

    // Print summary
    #[cfg(not(feature = "bench_fsst_ref"))]
    {
        println!(
            "Total Original Size: {:.2} MB",
            my_stats.total_original as f64 / 1024.0 / 1024.0
        );
        println!(
            "Weighted Avg Compression Ratio (My): {:.2}%",
            my_stats.avg_ratio()
        );
        println!("Total Throughput (My): {:.2} MB/s", my_stats.throughput_mbps());
    }

    #[cfg(any(feature = "bench_all", feature = "bench_fsst_ref"))]
    {
        #[cfg(feature = "bench_fsst_ref")]
        println!(
            "Total Original Size: {:.2} MB",
            ref_stats.total_original as f64 / 1024.0 / 1024.0
        );
        println!(
            "Weighted Avg Compression Ratio (Ref): {:.2}%",
            ref_stats.avg_ratio()
        );
        println!(
            "Total Throughput (Ref): {:.2} MB/s",
            ref_stats.throughput_mbps()
        );
    }

    #[cfg(feature = "bench_all")]
    {
        println!(
            "Speedup (Throughput): {:.2}x",
            my_stats.throughput_mbps() / ref_stats.throughput_mbps()
        );
    }
}
