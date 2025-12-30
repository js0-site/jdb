// JDB Slab Benchmark Library
// JDB Slab 性能评测库

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod engine;
mod error;
mod latency;
mod metrics;
mod runner;

mod adapter;

#[cfg(feature = "fjall")]
pub use adapter::fjall::FjallAdapter;
#[cfg(feature = "jdb_val")]
pub use adapter::jdb_val::JdbValAdapter;
#[cfg(feature = "rocksdb")]
pub use adapter::rocksdb::RocksDbAdapter;
pub use engine::{BenchEngine, dir_size};
pub use error::{Error, Result};
pub use jdb_bench_data::{
  AllCorpus, ByteZipfWorkload, EXPAND, KeyGen, LargeCorpus, MediumCorpus, MemBaseline, SEED,
  SmallCorpus, StrZipfWorkload, ZIPF_S, ZipfSampler, ZipfWorkload, load_all, process_mem,
};
pub use latency::{LatencyHistogram, LatencyStats};
pub use metrics::{BenchMetrics, SpaceMetrics};
pub use runner::{BenchConfig, BenchRunner, OpType, WorkloadType};
