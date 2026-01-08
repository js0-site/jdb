//! Table - Abstract table interface
//! 表 - 抽象表接口

pub mod mem;
mod meta;
mod order;
mod sst;

pub use meta::Meta;
pub use order::{Asc, Desc, Order};
pub use sst::SsTable;

/// Kv pair with boxed byte string key
/// 带盒装字节串 Key 的键值对
pub type Kv = (Box<[u8]>, crate::Pos);
