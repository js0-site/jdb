#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable sink (compaction) module
//! SSTable 下沉模块

mod error;
mod multi;
mod sinker;

pub use error::{Error, Result};
pub use jdb_sst::{Conf, Meta};
pub use multi::{new_asc, new_asc_from_refs, new_asc_no_discard};
pub use sinker::Sinker;
