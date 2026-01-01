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
  BlockBuilder, BlockIter, CompactMerger, CompactResult, DEFAULT_RESTART_INTERVAL, DataBlock,
  Entry, Index, Level, LevelMeta, Manifest, Memtable, MergeIter, MergedEntry, TableEntry,
  compact_l0_to_l1, compact_level, level_target_size, load_manifest, manifest_path,
  needs_l0_compaction, needs_level_compaction, save_manifest,
};
pub use sstable::{
  FOOTER_SIZE, Footer, SSTableIter, SSTableIterWithTombstones, TableInfo, TableMeta,
  Writer as SSTableWriter,
};
