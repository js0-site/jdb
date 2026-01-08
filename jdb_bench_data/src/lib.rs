// JDB Benchmark Data Library
// JDB 基准测试数据库

#![cfg_attr(docsrs, feature(doc_cfg))]

mod corpus;
mod error;
mod keygen;
mod mem;
mod stats;
mod workload;
mod zipf;

pub use corpus::{AllCorpus, LargeCorpus, MediumCorpus, SmallCorpus, load_all};
pub use error::{Error, Result};
pub use keygen::{EXPAND, KeyGen, SEED, ZIPF_S};
pub use mem::{Jemalloc, MemBaseline, process_mem};
pub use stats::{DatasetStats, SizeBucket, fmt_size};
pub use workload::{SizeTier, Workload, WorkloadConfig, load_workload};
pub use zipf::{ByteZipfWorkload, StrZipfWorkload, ZipfSampler, ZipfWorkload};
