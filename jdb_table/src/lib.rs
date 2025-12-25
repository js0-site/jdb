//! Database implementation for JDB
//! JDB 数据库实现

#![cfg_attr(docsrs, feature(doc_cfg))]

mod conf;
mod db;
mod error;

pub use conf::{DbConf, Retention};
pub use db::{Commit, Db};
pub use error::{Error, Result};
