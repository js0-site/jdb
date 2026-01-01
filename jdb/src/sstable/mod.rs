//! SSTable - Sorted String Table
//! 有序字符串表
//!
//! On-disk storage format for LSM-Tree.
//! LSM-Tree 的磁盘存储格式。

mod footer;
mod meta;
mod reader;
mod writer;

pub use footer::{Footer, FOOTER_SIZE, MAGIC};
pub use meta::TableMeta;
pub use reader::{Reader, SSTableIter, SSTableIterWithTombstones};
pub use writer::Writer;
