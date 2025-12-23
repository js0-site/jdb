//! JDB API - High-level Rust SDK
//! JDB API - 高级 Rust SDK

mod client;
mod error;

pub use client::JdbClient;
pub use error::{ApiError, Result};

// Re-export common types 重新导出常用类型
pub use jdb_comm::{TableID, Timestamp, VNodeID};
