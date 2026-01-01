#![cfg_attr(docsrs, feature(doc_cfg))]

//! jdb - High-performance embedded KV-separated database
//! jdb - 高性能嵌入式 KV 分离数据库

mod batch;
mod conf;
mod error;
mod index;
mod ns;
mod sstable;
mod watch;

pub use conf::{Conf, ConfItem};
pub use error::{Error, Result};
pub use index::{
  compact_l0_to_l1, compact_level, level_target_size, load_manifest, needs_l0_compaction,
  needs_level_compaction, save_manifest, BlockBuilder, BlockIter, CompactMerger, CompactResult,
  DataBlock, Entry, Index, Level, LevelMeta, Manifest, Memtable, MergeIter, MergedEntry,
  TableEntry, DEFAULT_RESTART_INTERVAL,
};
pub use sstable::{
  Footer, Reader as SSTableReader, SSTableIter, SSTableIterWithTombstones, TableMeta,
  Writer as SSTableWriter, FOOTER_SIZE, MAGIC,
};
