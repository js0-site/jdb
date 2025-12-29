//! Scalability benchmarks.
//! 可扩展性基准测试

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mimalloc::MiMalloc;

use autoscale_cuckoo_filter::ScalableCuckooFilter;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn add(c: &mut Criterion) {
    let mut group = c.benchmark_group("add");

    for precision in [0.1, 0.001, 0.0001, 0.00001] {
        let mut filter = ScalableCuckooFilter::<u64>::new(1_000_000, precision);
        let mut i = 0;

        group.bench_function(BenchmarkId::new("precision", precision), |b| {
            b.iter(|| {
                filter.add(&i);
                i += 1;
            })
        });
    }
}

fn contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains");

    for precision in [0.1, 0.001, 0.0001, 0.00001] {
        let mut filter = ScalableCuckooFilter::<u64>::new(1_000_000, precision);
        // Pre-fill filter for realistic benchmark
        // 预填充过滤器以进行真实基准测试
        for i in 0..10_000u64 {
            filter.add(&i);
        }

        group.bench_function(BenchmarkId::new("precision", precision), |b| {
            let mut i = 0u64;
            b.iter(|| {
                let r = filter.contains(&i);
                i = (i + 1) % 10_000;
                std::hint::black_box(r)
            })
        });
    }
}

criterion_group!(benches, add, contains);
criterion_main!(benches);
