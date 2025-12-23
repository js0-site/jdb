#![cfg_attr(docsrs, feature(doc_cfg))]

//! B+ Tree index B+ 树索引

mod btree;
mod node;

pub use btree::BTree;
pub use node::{Node, MAX_KEYS};
