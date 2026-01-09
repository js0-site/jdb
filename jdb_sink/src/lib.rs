#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable sink (compaction) module
//! SSTable 下沉模块

mod error;
mod log;
mod multi;
mod sinker;

pub use error::{Error, Result};
pub use jdb_sst::{Conf, Meta};
pub use log::{SINK_DIR, SinkCount, SinkLog, flush_positions};
pub use multi::{new_asc, new_asc_from_refs, new_asc_no_discard};
pub use sinker::{MergeResult, Sinker};
