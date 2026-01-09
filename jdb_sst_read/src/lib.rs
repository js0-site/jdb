#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable reader module
//! SSTable 读取模块

mod stream;
mod table;

pub use stream::{asc_stream, desc_stream, to_owned};
pub use table::Table;
