#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table
//! 有序字符串表

mod block;
mod compact;
mod conf;
mod consts;
mod error;
mod foot;
mod handle;
mod level;
mod load;
mod meta;
mod read;
pub mod stream;
mod table;
mod write;

pub(crate) use compact::Compactor;
pub use conf::{Conf, default};
pub use error::{Error, Result};
pub use handle::Handle;
pub use level::{HandleLevel, HandleLevels, SstLevel, SstLevels};
pub use load::load;
pub use meta::Meta;
pub use read::Read;
pub use stream::{MultiAsc, MultiDesc};
pub use table::Table;
pub use write::{gen_id, write, write_stream, write_with_id};
