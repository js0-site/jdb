//! Block format with prefix compression
//! 带前缀压缩的块格式

mod builder;
mod data;
mod iter;

pub(crate) use builder::BlockBuilder;
pub use data::DataBlock;
pub(crate) use iter::{last_key, read_entry, restore_key};
