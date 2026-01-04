//! Block format with prefix compression
//! 带前缀压缩的块格式

mod builder;
mod data;
mod iter;

pub(crate) use builder::BlockBuilder;
pub(crate) use data::DataBlock;
pub(crate) use iter::{last_key, read_entry, restore_key};

/// Default restart interval
/// 默认重启间隔
pub(crate) const DEFAULT_RESTART_INTERVAL: usize = 16;
