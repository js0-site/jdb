use std::{collections::HashMap, fs::File, io::Write, sync::Mutex};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[allow(unused_imports)]
use jdb_pef::Pef;

// Global stats storage
static STATS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

mod traits;
use traits::Bench;

// ---------------------------
// Wrapper for jdb_pef
// ---------------------------

fn record_stat(key: &str, val: String) {
  let mut stats = STATS.lock().unwrap();
  if stats.is_none() {
    *stats = Some(HashMap::new());
  }
  stats.as_mut().unwrap().insert(key.to_string(), val);
}

#[cfg(feature = "bench-jdb")]
struct JdbPefBench(Pef);

#[cfg(feature = "bench-jdb")]
impl Bench for JdbPefBench {
  const NAME: &'static str = "jdb_pef";
  type Iter<'a> = jdb_pef::Iter<'a>;

  fn new(data: &[u64]) -> Self {
    Self(Pef::new(data))
  }

  fn size_in_bytes(&self) -> usize {
    self.0.memory_usage()
  }

  fn get(&self, index: usize) -> Option<u64> {
    self.0.get(index)
  }

  fn next_ge(&self, target: u64) -> Option<u64> {
    self.0.next_ge(target)
  }

  fn iter<'a>(&'a self) -> Self::Iter<'a> {
    self.0.iter()
  }
}

// ---------------------------
// Wrapper for sucds
// ---------------------------

#[cfg(feature = "bench-sucds")]
use sucds::mii_sequences::{EliasFano, EliasFanoBuilder};

#[cfg(feature = "bench-sucds")]
struct SucdsBench(EliasFano);

#[cfg(feature = "bench-sucds")]
impl Bench for SucdsBench {
  const NAME: &'static str = "sucds";
  type Iter<'a> = SucdsIter<'a>;

  fn new(data: &[u64]) -> Self {
    let max_val = *data.last().unwrap_or(&0) as usize;
    let mut builder = EliasFanoBuilder::new(max_val + 100, data.len()).unwrap();
    builder.extend(data.iter().map(|&x| x as usize)).unwrap();
    Self(builder.build())
  }

  fn size_in_bytes(&self) -> usize {
    sucds::Serializable::size_in_bytes(&self.0)
  }

  fn get(&self, index: usize) -> Option<u64> {
    if index >= self.0.len() {
      return None;
    }
    self.0.select(index).map(|v| v as u64)
  }

  #[allow(clippy::manual_find)]
  fn next_ge(&self, target: u64) -> Option<u64> {
    // Crash mitigation: linear scan until a proper fix for rank() panic is found.
    for v in self.iter() {
      if v >= target {
        return Some(v);
      }
    }
    None
  }

  fn iter<'a>(&'a self) -> Self::Iter<'a> {
    SucdsIter {
      ef: &self.0,
      idx: 0,
      len: self.0.len(),
    }
  }
}

#[cfg(feature = "bench-sucds")]
pub struct SucdsIter<'a> {
  ef: &'a EliasFano,
  idx: usize,
  len: usize,
}

#[cfg(feature = "bench-sucds")]
impl<'a> Iterator for SucdsIter<'a> {
  type Item = u64;
  fn next(&mut self) -> Option<Self::Item> {
    if self.idx >= self.len {
      None
    } else {
      let val = self.ef.select(self.idx).map(|v| v as u64);
      self.idx += 1;
      val
    }
  }
}

// ---------------------------
// Benchmark Logic
// ---------------------------

// Helper to configure benchmark group with standard settings
fn configure_group<'a>(
  c: &'a mut Criterion,
  group_name: &str,
  throughput_elements: u64,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
  let mut group = c.benchmark_group(group_name);
  group.measurement_time(std::time::Duration::from_secs(1));
  group.warm_up_time(std::time::Duration::from_millis(100));
  group.throughput(Throughput::Elements(throughput_elements));
  group
}

fn bench_ops<B: Bench>(c: &mut Criterion, data: &[u64], group_name: &str) {
  if data.is_empty() {
    return;
  }

  let struct_name = B::NAME;
  let _full_group_name = format!("{}/{}", group_name, struct_name);

  {
    // 1. Measure Construction & Compression
    let structure = B::new(data);

    // Calculate BPE (Bits Per Element)
    let total_bits = structure.size_in_bytes() * 8;
    let bpe = total_bits as f64 / data.len() as f64;
    let size_mb = structure.size_in_bytes() as f64 / 1024.0 / 1024.0;
    let compression_pct = (bpe / 64.0) * 100.0;

    println!(
      "[{}] Compression: {:.2}% ({:.2} bits/element, Total: {:.2} MB)",
      struct_name, compression_pct, bpe, size_mb
    );

    record_stat(&format!("{}.bpe", struct_name), format!("{:.3}", bpe));
    record_stat("params.n", data.len().to_string());

    // Benchmark Random Access (Get)
    let mut group = configure_group(c, "Random Access", 1);
    let mut rng = fastrand::Rng::with_seed(42);
    group.bench_function(BenchmarkId::new("get", struct_name), |b| {
      b.iter(|| {
        let idx = rng.usize(0..data.len());
        structure.get(idx)
      })
    });
    group.finish();

    // Pef benchmarks
    #[cfg(feature = "bench-jdb")]
    if struct_name == "jdb_pef" {
      let mut group = configure_group(c, "Range", 1);
      let _max_val = *data.last().unwrap(); // max_val needs to be defined for this scope
      let _min_val = *data.first().unwrap_or(&0); // Define min_val for the scope
      // The original `pef` variable was for the "Range Forward" benchmark.
      // The new benchmark constructs `Pef` inside the `iter_batched` setup closure.
      let n = data.len(); // Capture n for the new benchmark
      group.bench_function(BenchmarkId::new("Random Access", _full_group_name), |b| {
        b.iter_batched(
          || {
            let pef = Pef::new(data);
            let mut rng = fastrand::Rng::with_seed(42); // New rng for this batch
            let indices: Vec<usize> = (0..10_000).map(|_| rng.usize(0..n)).collect();
            (pef, indices)
          },
          |(pef_ref, indices)| {
            // Receive the reference
            for idx in indices {
              pef_ref.get(idx);
            }
          },
          criterion::BatchSize::SmallInput,
        )
      });
      group.finish();
    }

    // Benchmark Search (Next GE)
    let mut group = configure_group(c, "Search", 1);
    let mut rng = fastrand::Rng::with_seed(42);
    let max_val = *data.last().unwrap();
    group.bench_function(BenchmarkId::new("next_ge", struct_name), |b| {
      b.iter(|| {
        let target = rng.u64(0..=max_val);
        structure.next_ge(target)
      })
    });
    group.finish();

    // Benchmark Sequential Access (Iterator)
    let mut group = configure_group(c, "Sequential", data.len() as u64);
    group.bench_function(BenchmarkId::new("iter", struct_name), |b| {
      b.iter(|| {
        // Consume the whole iterator
        let count = structure.iter().count();
        debug_assert_eq!(count, data.len());
        count
      })
    });
    group.finish();
  }
}

fn run_benchmarks(c: &mut Criterion) {
  // Generate Dataset
  let n = 50_000;
  println!(
    "Generating {} sorted integers (simulating SST offsets, avg gap ~64)...",
    n
  );
  let mut data = Vec::with_capacity(n);
  let mut val = 0;
  let mut rng = fastrand::Rng::with_seed(42);
  for _ in 0..n {
    // Average key-value size ~64 bytes
    val += rng.u64(32..96);
    data.push(val);
  }

  #[cfg(feature = "bench-jdb")]
  bench_ops::<JdbPefBench>(c, &data, "All");

  #[cfg(feature = "bench-sucds")]
  bench_ops::<SucdsBench>(c, &data, "All");
  #[cfg(feature = "bench-jdb")]
  {
    bench_jdb_advanced(c, &data);
  }

  // Save stats
  let stats = STATS.lock().unwrap();
  if let Some(map) = &*stats {
    let mut json = String::from("{");
    for (i, (k, v)) in map.iter().enumerate() {
      if i > 0 {
        json.push_str(", ");
      }
      json.push_str(&format!("\"{}\": \"{}\"", k, v));
    }
    json.push('}');
    let _ = File::create("benches/stats.json").and_then(|mut f| f.write_all(json.as_bytes()));
  }
}

#[cfg(feature = "bench-jdb")]
fn bench_jdb_advanced(c: &mut Criterion, data: &[u64]) {
  use jdb_pef::Pef;
  let pef = Pef::new(data);

  // Construct PEF
  // 1. Reverse Iteration
  {
    let mut group = configure_group(c, "Sequential", data.len() as u64);
    group.bench_function(BenchmarkId::new("Iterate Reverse", "jdb_pef"), |b| {
      b.iter_batched(
        || Pef::new(data),
        |pef| pef.rev_iter().count(),
        criterion::BatchSize::SmallInput,
      )
    });
    group.finish();
  }

  // Pef benchmarks
  {
    let mut group = configure_group(c, "Search", 1); // Assuming "Get Next" is a search operation
    let mut rng = fastrand::Rng::with_seed(42);
    let max_val = *data.last().unwrap();
    group.bench_function(BenchmarkId::new("Get Next", "jdb_pef"), |b| {
      b.iter_batched(
        || {
          let pef = Pef::new(data);
          let queries: Vec<u64> = (0..1000).map(|_| rng.u64(0..max_val)).collect();
          (pef, queries)
        },
        |(pef, queries)| {
          for &q in &queries {
            pef.next_ge(q); // Assuming 'Get Next' refers to next_ge
          }
        },
        criterion::BatchSize::SmallInput,
      )
    });
    group.finish();
  }

  // Ranges setup
  let start = data[data.len() / 4];
  let end = data[data.len() * 3 / 4];
  let count_expected = data.iter().filter(|&&x| x >= start && x < end).count();

  // 2. Range Forward
  {
    let mut group = configure_group(c, "Range", count_expected as u64);
    group.bench_function(BenchmarkId::new("range_iter", "jdb_pef"), |b| {
      b.iter(|| pef.range(start..end).count())
    });
    group.finish();
  }

  // 3. Range Reverse
  {
    let mut group = configure_group(c, "Range", count_expected as u64);
    group.bench_function(BenchmarkId::new("rev_range_iter", "jdb_pef"), |b| {
      b.iter(|| pef.rev_range(start..end).count())
    });
    group.finish();
  }
}

criterion_group!(benches, run_benchmarks);
criterion_main!(benches);
