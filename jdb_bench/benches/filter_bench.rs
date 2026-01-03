//! Cuckoo filter benchmark
//! 布谷鸟过滤器性能测试

use std::time::Instant;

use autoscale_cuckoo_filter::CuckooFilter;

const N: usize = 1_000_000;
const ITER: usize = 10_000_000;

fn main() {
  println!("=== autoscale_cuckoo_filter ===");

  // Insert
  let mut cf: CuckooFilter<usize> = CuckooFilter::new(N, 0.001);
  let start = Instant::now();
  for i in 0..N {
    cf.add(&i);
  }
  let insert_time = start.elapsed();
  println!(
    "  insert {N}: {:?} ({:.0} ops/s)",
    insert_time,
    N as f64 / insert_time.as_secs_f64()
  );

  // Contains (hit)
  let start = Instant::now();
  let mut hits = 0u64;
  for i in 0..ITER {
    if cf.contains(&(i % N)) {
      hits += 1;
    }
  }
  let hit_time = start.elapsed();
  println!(
    "  contains (hit) {ITER}: {:?} ({:.0} ops/s), hits={hits}",
    hit_time,
    ITER as f64 / hit_time.as_secs_f64()
  );

  // Contains (miss)
  let start = Instant::now();
  let mut misses = 0u64;
  for i in N..(N + ITER) {
    if !cf.contains(&i) {
      misses += 1;
    }
  }
  let miss_time = start.elapsed();
  println!(
    "  contains (miss) {ITER}: {:?} ({:.0} ops/s), misses={misses}",
    miss_time,
    ITER as f64 / miss_time.as_secs_f64()
  );
}
