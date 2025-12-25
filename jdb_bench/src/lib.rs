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
#[cfg(feature = "jdb")]
pub use adapter::jdb::JdbSlabAdapter;
#[cfg(feature = "rocksdb")]
pub use adapter::rocksdb::RocksDbAdapter;
#[cfg(feature = "sled")]
pub use adapter::sled::SledAdapter;
pub use corpus::{
  AllCorpus, LargeTextCorpus, MediumTextCorpus, SmallNumCorpus, load_all, load_large_text,
  load_medium_text, load_small_num,
};
pub use engine::{BenchEngine, dir_size, process_memory};
pub use error::{Error, Result};
pub use latency::{LatencyHistogram, LatencyStats};
pub use metrics::{BenchMetrics, SpaceMetrics};
pub use runner::{BenchConfig, BenchRunner, OpType, WorkloadType};
pub use zipf::{ByteZipfWorkload, StrZipfWorkload, ZipfWorkload};
