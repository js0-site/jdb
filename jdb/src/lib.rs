#![cfg_attr(docsrs, feature(doc_cfg))]

//! jdb - High-performance embedded KV-separated database
//! jdb - 高性能嵌入式 KV 分离数据库

mod batch;
mod conf;
mod error;
mod index;
mod jdb;
mod ns;
mod ns_mgr;
mod sstable;
mod watch;

pub use batch::{Batch, Op};
pub use conf::{Conf, ConfItem};
pub use error::{Error, Result};
pub use index::{
  BlockBuilder, BlockIter, CompactMerger, CompactResult, DEFAULT_RESTART_INTERVAL, DataBlock,
  Entry, Index, Level, LevelMeta, Manifest, Memtable, MergeIter, MergedEntry, TableEntry,
  compact_l0_to_l1, compact_level, level_target_size, load_manifest, manifest_path,
  needs_l0_compaction, needs_level_compaction, save_manifest,
};
pub use jdb::Jdb;
pub use ns::{Ns, Site};
pub use ns_mgr::{NsId, NsIndex, NsMgr};
pub use sstable::{
  FOOTER_SIZE, Footer, SSTableIter, TableInfo, TableMeta, Writer as SSTableWriter, key_to_u64,
};
