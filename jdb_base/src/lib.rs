//! jdb_base - Shared types for jdb
//! jdb 共享类型
//!
//! Contains Flag, Pos and other common types.
//! 包含 Flag、Pos 等公共类型。

mod ckp;
mod flag;
mod pos;

pub use ckp::{Ckp, WalId, WalIdOffset, WalOffset};
pub use flag::Flag;
pub use pos::Pos;
