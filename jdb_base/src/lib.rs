//! jdb_base - Shared types for jdb
//! jdb 共享类型
//!
//! Contains Flag, Pos, table and other common types.
//! 包含 Flag、Pos、table 等公共类型。

pub mod ckp;
mod flag;
mod pos;
pub mod table;

pub use ckp::{Ckp, WalId, WalIdOffset, WalOffset};
pub use flag::Flag;
pub use pos::Pos;
