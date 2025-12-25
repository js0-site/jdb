//! CoW B+ Tree with prefix compression
//! 前缀压缩的 CoW B+ 树

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
mod node;
mod tree;

pub use error::{Error, Result};
pub use node::{Internal, Leaf, Node, PageId, MAX_KEYS};
pub use tree::BTree;
