#![cfg_attr(docsrs, feature(doc_cfg))]

//! Write Ahead Log 预写日志

mod writer;

pub use writer::{WalReader, WalWriter};
