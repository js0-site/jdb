#![cfg_attr(docsrs, feature(doc_cfg))]

//! 高性能 NVMe B+ 树索引
//! High-performance NVMe B+ tree index

mod cursor;
mod key;
mod tree;
mod view;

pub use cursor::Cursor;
pub use key::Key;
pub use tree::BTree;
pub use view::{InternalView, LeafMut, LeafView};
