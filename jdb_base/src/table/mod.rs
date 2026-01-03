//! table - Abstract table interface
//! 表抽象接口

mod kv;
mod merge;

pub use kv::{Kv, Table, TableMut};
pub use merge::MergeIter;
