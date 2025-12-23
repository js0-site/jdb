#![cfg_attr(docsrs, feature(doc_cfg))]

//! Value Log for KV separation 值日志（KV 分离）

mod writer;

pub use writer::{VlogReader, VlogWriter};
