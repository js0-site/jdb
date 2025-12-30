// LRU cache performance comparison benchmark
// LRU 缓存性能对比基准测试

use std::{io::Write, path::Path, time::Instant};

use jdb_bench_data::{MemBaseline, SEED, ZIPF_S, load_all};
use mimalloc::MiMalloc;
use serde::Serialize;
use size_lru::SizeLru;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DATA_DIR: &str = "../jdb_bench_data/data";
const JSON_PATH: &str = "bench.json";
const LARGE_CAP: u64 = 64 * 1024 * 1024;
const MEDIUM_CAP: u64 = 16 * 1024 * 1024;
const SMALL_CAP: u64 = 1024 * 1024;
const LOOPS: usize = 3;

#[derive(Serialize)]
struct BenchResult {
  lib: String,
  get_mops: f64,
  set_mops: f64,
  hit_rate: f64,
  memory_kb: f64,
}

#[derive(Serialize)]
struct CategoryResult {
  name: String,
  capacity_mb: f64,
  items: usize,
  results: Vec<BenchResult>,
}

#[derive(Serialize)]
struct BenchOutput {
  categories: Vec<CategoryResult>,
}

type KvPair = (Vec<u8>, Vec<u8>);

fn to_kv(data: &[(String, Vec<u8>)]) -> Vec<KvPair> {
  data.iter().map(|(k, v)| (k.as_bytes().to_vec(), v.clone())).collect()
}

#[inline]
fn ns_to_mops(ns: f64) -> f64 { 1000.0 / ns }

trait LruBench {
  fn name(&self) -> &'static str;
  fn set(&mut self, key: &[u8], val: &[u8]);
  fn get(&mut self, key: &[u8]) -> bool;
}


// size_lru
struct SizeLruAdapter(size_lru::Lhd<Vec<u8>, Vec<u8>>);
impl SizeLruAdapter {
  fn new(cap: u64) -> Self { Self(size_lru::Lhd::new(cap as usize)) }
}
impl LruBench for SizeLruAdapter {
  fn name(&self) -> &'static str { "size_lru" }
  fn set(&mut self, key: &[u8], val: &[u8]) {
    self.0.set(key.to_vec(), val.to_vec(), val.len() as u32);
  }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}

// moka
struct MokaAdapter(moka::sync::Cache<Vec<u8>, Vec<u8>>);
impl MokaAdapter {
  fn new(cap: u64) -> Self {
    Self(moka::sync::Cache::builder()
      .weigher(|_k: &Vec<u8>, v: &Vec<u8>| v.len() as u32)
      .max_capacity(cap).build())
  }
}
impl LruBench for MokaAdapter {
  fn name(&self) -> &'static str { "moka" }
  fn set(&mut self, key: &[u8], val: &[u8]) { self.0.insert(key.to_vec(), val.to_vec()); }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}

// mini-moka
struct MiniMokaAdapter(mini_moka::sync::Cache<Vec<u8>, Vec<u8>>);
impl MiniMokaAdapter {
  fn new(cap: u64) -> Self {
    Self(mini_moka::sync::Cache::builder()
      .weigher(|_k: &Vec<u8>, v: &Vec<u8>| v.len() as u32)
      .max_capacity(cap).build())
  }
}
impl LruBench for MiniMokaAdapter {
  fn name(&self) -> &'static str { "mini-moka" }
  fn set(&mut self, key: &[u8], val: &[u8]) { self.0.insert(key.to_vec(), val.to_vec()); }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}

// clru
struct ValScale;
impl clru::WeightScale<Vec<u8>, Vec<u8>> for ValScale {
  fn weight(&self, _key: &Vec<u8>, val: &Vec<u8>) -> usize { val.len() }
}
struct ClruAdapter(clru::CLruCache<Vec<u8>, Vec<u8>, std::collections::hash_map::RandomState, ValScale>);
impl ClruAdapter {
  fn new(cap: u64) -> Self {
    Self(clru::CLruCache::with_config(
      clru::CLruCacheConfig::new(std::num::NonZeroUsize::new(cap as usize).expect("cap > 0"))
        .with_scale(ValScale)))
  }
}
impl LruBench for ClruAdapter {
  fn name(&self) -> &'static str { "clru" }
  fn set(&mut self, key: &[u8], val: &[u8]) { let _ = self.0.put_with_weight(key.to_vec(), val.to_vec()); }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}

// lru
struct LruAdapter(lru::LruCache<Vec<u8>, Vec<u8>>);
impl LruAdapter {
  fn new(cap: usize) -> Self {
    Self(lru::LruCache::new(std::num::NonZeroUsize::new(cap).expect("cap > 0")))
  }
}
impl LruBench for LruAdapter {
  fn name(&self) -> &'static str { "lru" }
  fn set(&mut self, key: &[u8], val: &[u8]) { self.0.put(key.to_vec(), val.to_vec()); }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}

// hashlink
struct HashlinkAdapter(hashlink::LruCache<Vec<u8>, Vec<u8>>);
impl HashlinkAdapter {
  fn new(cap: usize) -> Self { Self(hashlink::LruCache::new(cap)) }
}
impl LruBench for HashlinkAdapter {
  fn name(&self) -> &'static str { "hashlink" }
  fn set(&mut self, key: &[u8], val: &[u8]) { self.0.insert(key.to_vec(), val.to_vec()); }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}

// schnellru
struct SchnellruAdapter(schnellru::LruMap<Vec<u8>, Vec<u8>, schnellru::ByLength>);
impl SchnellruAdapter {
  fn new(cap: u32) -> Self { Self(schnellru::LruMap::new(schnellru::ByLength::new(cap))) }
}
impl LruBench for SchnellruAdapter {
  fn name(&self) -> &'static str { "schnellru" }
  fn set(&mut self, key: &[u8], val: &[u8]) { self.0.insert(key.to_vec(), val.to_vec()); }
  fn get(&mut self, key: &[u8]) -> bool { self.0.get(&key.to_vec()).is_some() }
}
