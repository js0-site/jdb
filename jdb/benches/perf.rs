//! Performance benchmark 性能测试

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use jdb::JdbClient;

const BENCH_DIR: &str = "/tmp/jdb_bench";

fn setup_client() -> JdbClient {
  std::fs::remove_dir_all(BENCH_DIR).ok();
  JdbClient::open(BENCH_DIR).expect("open")
}

fn bench_put(c: &mut Criterion) {
  let rt = tokio::runtime::Runtime::new().unwrap();

  let mut group = c.benchmark_group("put");
  group.throughput(Throughput::Elements(1));

  for size in [64, 256, 1024, 4096].iter() {
    group.bench_with_input(BenchmarkId::new("value_size", size), size, |b, &size| {
      let client = setup_client();
      let val = vec![0u8; size];

      b.iter(|| {
        rt.block_on(async {
          let key = fastrand::u64(..).to_le_bytes();
          client.put(b"bench", &key, black_box(&val)).await.unwrap();
        });
      });

      client.close();
    });
  }
  group.finish();

  std::fs::remove_dir_all(BENCH_DIR).ok();
}

fn bench_get(c: &mut Criterion) {
  let rt = tokio::runtime::Runtime::new().unwrap();

  let mut group = c.benchmark_group("get");
  group.throughput(Throughput::Elements(1));

  let client = setup_client();

  // Prepare data 准备数据
  rt.block_on(async {
    for i in 0..1000u64 {
      let key = i.to_le_bytes();
      let val = vec![i as u8; 256];
      client.put(b"bench", &key, &val).await.unwrap();
    }
  });

  group.bench_function("hit", |b| {
    b.iter(|| {
      rt.block_on(async {
        let i = fastrand::u64(0..1000);
        let key = i.to_le_bytes();
        black_box(client.get(&key).await.unwrap());
      });
    });
  });

  group.bench_function("miss", |b| {
    b.iter(|| {
      rt.block_on(async {
        let key = fastrand::u64(..).to_le_bytes();
        black_box(client.get(&key).await.unwrap());
      });
    });
  });

  group.finish();
  client.close();
  std::fs::remove_dir_all(BENCH_DIR).ok();
}

fn bench_range(c: &mut Criterion) {
  let rt = tokio::runtime::Runtime::new().unwrap();

  let mut group = c.benchmark_group("range");

  let client = setup_client();

  // Prepare sorted keys 准备有序键
  rt.block_on(async {
    for i in 0..10000u64 {
      let key = format!("k{i:08}").into_bytes();
      let val = vec![0u8; 128];
      client.put(b"bench", &key, &val).await.unwrap();
    }
  });

  for range_size in [10, 100, 1000].iter() {
    group.throughput(Throughput::Elements(*range_size as u64));
    group.bench_with_input(
      BenchmarkId::new("size", range_size),
      range_size,
      |b, &size| {
        b.iter(|| {
          rt.block_on(async {
            let start = fastrand::u64(0..(10000 - size) as u64);
            let end = start + size as u64;
            let s = format!("k{start:08}").into_bytes();
            let e = format!("k{end:08}").into_bytes();
            black_box(client.range(&s, &e).await.unwrap());
          });
        });
      },
    );
  }

  group.finish();
  client.close();
  std::fs::remove_dir_all(BENCH_DIR).ok();
}

fn bench_mixed(c: &mut Criterion) {
  let rt = tokio::runtime::Runtime::new().unwrap();

  let mut group = c.benchmark_group("mixed");
  group.throughput(Throughput::Elements(100));

  let client = setup_client();

  // 80% read, 20% write workload
  group.bench_function("80r_20w", |b| {
    b.iter(|| {
      rt.block_on(async {
        for _ in 0..100 {
          if fastrand::u8(0..100) < 80 {
            let key = fastrand::u64(..).to_le_bytes();
            black_box(client.get(&key).await.unwrap());
          } else {
            let key = fastrand::u64(..).to_le_bytes();
            let val = vec![0u8; 256];
            client.put(b"bench", &key, &val).await.unwrap();
          }
        }
      });
    });
  });

  group.finish();
  client.close();
  std::fs::remove_dir_all(BENCH_DIR).ok();
}

criterion_group!(benches, bench_put, bench_get, bench_range, bench_mixed);
criterion_main!(benches);
