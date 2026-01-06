//! Table - Abstract table interface
//! 表 - 抽象表接口

mod kv;
mod sstable;

pub use kv::{Kv, Table, TableMut, prefix_end};
pub use sstable::SsTable;
