//! SSTable - Sorted String Table
//! 有序字符串表
//!
//! On-disk storage format for LSM-Tree.
//! LSM-Tree 的磁盘存储格式。

mod footer;
mod meta;
mod reader;
mod writer;

pub use footer::{FOOTER_SIZE, Footer};
pub use meta::TableMeta;
pub use reader::{SSTableIter, SSTableIterWithTombstones, TableInfo};
pub use writer::Writer;
