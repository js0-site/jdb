#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table
//! 有序字符串表

mod block;
mod conf;
mod consts;
mod error;
mod foot;
mod stream;
mod table;
mod write;

pub use block::DataBlock;
pub use conf::{Conf, default};
pub use consts::{HOUR_MS, TMP_DIR};
pub use error::{Error, Result};
pub use jdb_base::sst::Meta;
pub use stream::{asc_stream, desc_stream, to_owned};
pub use table::Table;
pub use write::{Write, gen_id, write, write_id, write_stream};
