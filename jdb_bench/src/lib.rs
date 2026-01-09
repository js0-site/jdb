// JDB Slab Benchmark Library
// JDB Slab 性能评测库

use jdb_bench_data::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod engine;
mod error;
mod latency;
mod metrics;
mod runner;

mod adapter;

#[cfg(feature = "fjall")]
pub use adapter::fjall::FjallAdapter;
#[cfg(feature = "rocksdb")]
pub use adapter::rocksdb::RocksDbAdapter;
#[cfg(feature = "wlog")]
pub use adapter::wlog::JdbValAdapter;
pub use engine::{BenchEngine, dir_size};
pub use error::{Error, Result};
pub use latency::{LatencyHistogram, LatencyStats};
pub use metrics::{BenchMetrics, SpaceMetrics};
pub use runner::{BenchConfig, BenchRunner, OpType, WorkloadType};
