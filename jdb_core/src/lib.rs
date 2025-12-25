//! JDB Core - top level database management
//! JDB 核心 - 顶层数据库管理

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
mod jdb;

pub use error::{Error, Result};
pub use jdb::Jdb;

// Re-export Db for convenience / 重导出 Db
pub use jdb_db::Db;
