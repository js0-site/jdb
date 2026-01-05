#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table
//! 有序字符串表

mod block;
mod error;
mod footer;
mod load;
mod meta;
mod reader;
mod stream;
mod table;
mod writer;

pub use error::{Error, Result};
pub use load::load;
pub use meta::TableMeta;
pub use reader::TableInfo;
pub use stream::{AscStream, DescStream};
pub use table::SSTable;
pub use writer::{DEFAULT_BLOCK_SIZE, Writer};
