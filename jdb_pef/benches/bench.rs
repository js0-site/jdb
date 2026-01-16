use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
#[allow(unused_imports)]
use jdb_pef::Pef;
use rand::Rng;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod traits;
use traits::Bench;

// ---------------------------
// Memory Profiling
// ---------------------------

struct MemProfiler {
    allocated: tikv_jemalloc_ctl::stats::allocated,
}

impl MemProfiler {
    fn new() -> Self {
        Self {
            allocated: tikv_jemalloc_ctl::stats::allocated::mib().unwrap(),
        }
    }

    fn read_allocated(&self) -> usize {
        // Advance epoch to update stats
        tikv_jemalloc_ctl::epoch::advance().unwrap();
        // Read allocated bytes
        self.allocated.read().unwrap()
    }
}

// ---------------------------
// Wrapper for jdb_pef
// ---------------------------

#[cfg(feature = "bench-jdb")]
struct JdbPefBench(Pef);

#[cfg(feature = "bench-jdb")]
impl Bench for JdbPefBench {
    const NAME: &'static str = "jdb_pef";

    fn new(data: &[u64]) -> Self {
        Self(Pef::new(data, 128))
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
}

// ---------------------------
// Wrapper for sucds
// ---------------------------

#[cfg(feature = "bench-sucds")]
struct SucdsBench(sucds::EliasFano<sucds::util::VecIO>);

#[cfg(feature = "bench-sucds")]
impl Bench for SucdsBench {
    const NAME: &'static str = "sucds";

    fn new(data: &[u64]) -> Self {
        // sucds requires an iterator builder
        let mut builder = sucds::EliasFanoBuilder::new(
             *data.last().unwrap_or(&0) as usize, 
             data.len()
        ).unwrap();
        builder.extend(data.iter().map(|&x| x as usize)).unwrap();
        Self(builder.build().unwrap())
    }

    fn size_in_bytes(&self) -> usize {
        sucds::Serializable::size_in_bytes(&self.0)
    }

    fn get(&self, index: usize) -> Option<u64> {
        use sucds::EliasFanoList;
        if index >= self.0.len() {
             return None;
        }
        Some(self.0.select(index) as u64)
    }

    fn next_ge(&self, target: u64) -> Option<u64> {
        use sucds::EliasFanoList;
        use sucds::EliasFanoRank;
        let r = EliasFanoRank::rank(&self.0, target as usize);
        if r >= self.0.len() {
            None 
        } else {
            // Check if value at rank is >= target
             // If target exists, rank returns index of first instance. select(index) == target.
             // If target doesn't exist, rank returns index of first element > target (insertion point).
             // select(index) should be > target.
             // Wait, rank(k) = count of elements < k.
             
             // Example: [10, 20]. rank(15) -> 1. select(1) -> 20. Correct.
             // Example: [10, 20]. rank(5) -> 0. select(0) -> 10. Correct.
             // Example: [10, 20]. rank(25) -> 2. select(2) -> OOB.
            // Check OOB
            if r >= self.0.len() {
                return None;
            }
            let val = self.0.select(r) as u64;
            Some(val)
        }
    }
}

// ---------------------------
// Benchmark Logic
// ---------------------------

fn bench_ops<B: Bench>(c: &mut Criterion, data: &[u64], group_name: &str) {
    if data.is_empty() { return; }

    let struct_name = B::NAME;
    let _full_group_name = format!("{}/{}", group_name, struct_name);

    {
        // 1. Measure Construction Memory & Time
        // We will measure memory manually.
        let profiler = MemProfiler::new();
        
        let start_mem = profiler.read_allocated();
        let structure = B::new(data);
        let end_mem = profiler.read_allocated();
        
        let mem_usage = (end_mem as isize - start_mem as isize).max(0) as usize;
        // Print memory usage in human readable format
        println!("[{}] Memory Usage: {:.2} MB (Self-reported: {:.2} MB)", 
            struct_name, 
            mem_usage as f64 / 1024.0 / 1024.0,
            structure.size_in_bytes() as f64 / 1024.0 / 1024.0
        );

        // Benchmark Random Access (Get)
        let mut group = c.benchmark_group("Random Access");
        let mut rng = rand::rng();
        group.bench_function(BenchmarkId::new("get", struct_name), |b| {
            b.iter(|| {
                let idx = rng.random_range(0..data.len());
                structure.get(idx)
            })
        });
        group.finish();

        // Benchmark Search (Next GE)
        let mut group = c.benchmark_group("Search");
         let mut rng = rand::rng();
        let max_val = *data.last().unwrap();
        group.bench_function(BenchmarkId::new("next_ge", struct_name), |b| {
             b.iter(|| {
                 let target = rng.random_range(0..=max_val);
                 structure.next_ge(target)
             })
        });
        group.finish();
    }
}


fn run_benchmarks(c: &mut Criterion) {
    // Generate Dataset
    let n = 1_000_000;
    println!("Generating {} sorted integers...", n);
    let mut data = Vec::with_capacity(n);
    let mut val = 0;
    let mut rng = rand::rng();
    for _ in 0..n {
        val += rng.random_range(1..10);
        data.push(val);
    }

    #[cfg(feature = "bench-jdb")]
    bench_ops::<JdbPefBench>(c, &data, "All");

    #[cfg(feature = "bench-sucds")]
    bench_ops::<SucdsBench>(c, &data, "All");
}

criterion_group!(benches, run_benchmarks);
criterion_main!(benches);
