#![cfg_attr(docsrs, feature(doc_cfg))]

// jdb - High-performance embedded KV-separated database
// jdb - 高性能嵌入式 KV 分离数据库

mod batch;
mod conf;
mod error;
mod index;
mod ns;
mod watch;

pub use conf::{Conf, ConfItem};
pub use error::{Error, Result};
