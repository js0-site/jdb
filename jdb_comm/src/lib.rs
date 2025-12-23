#![cfg_attr(docsrs, feature(doc_cfg))]

mod config;
mod consts;
mod error;
mod hash;
mod types;

pub use config::KernelConfig;
pub use consts::{FILE_MAGIC, INVALID_PAGE_ID, PAGE_HEADER_SIZE, PAGE_SIZE};
pub use error::{JdbError, JdbResult};
pub use hash::{fast_hash128, fast_hash64, route_to_vnode};
pub use types::{Lsn, PageID, TableID, Timestamp, VNodeID};
