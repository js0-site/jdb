#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table
//! 有序字符串表

mod block;
mod conf;
mod error;
mod footer;
mod load;
mod meta;
mod read;
mod reader;
mod stream;
mod writer;

pub use conf::{Conf, default};
pub use error::{Error, Result};
pub use load::load;
pub use meta::TableMeta;
pub use read::Read;
pub use reader::TableInfo;
pub use writer::new;
