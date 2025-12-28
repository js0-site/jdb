// JDB Slab Benchmark Library
// JDB Slab 性能评测库

mod corpus;
mod engine;
mod error;
mod latency;
mod metrics;
mod runner;
mod zipf;

mod adapter;

#[cfg(feature = "fjall")]
pub use adapter::fjall::FjallAdapter;
#[cfg(feature = "jdb_val")]
pub use adapter::jdb_val::JdbValAdapter;
#[cfg(feature = "rocksdb")]
pub use adapter::rocksdb::RocksDbAdapter;
pub use corpus::{AllCorpus, LargeCorpus, MediumCorpus, SmallCorpus, load_all};
pub use engine::{BenchEngine, dir_size, process_memory};
pub use error::{Error, Result};
pub use latency::{LatencyHistogram, LatencyStats};
pub use metrics::{BenchMetrics, SpaceMetrics};
pub use runner::{BenchConfig, BenchRunner, OpType, WorkloadType};
pub use zipf::{ByteZipfWorkload, StrZipfWorkload, ZipfWorkload};
