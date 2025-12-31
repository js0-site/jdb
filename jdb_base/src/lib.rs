//! jdb_base - Shared types for jdb
//! jdb 共享类型
//!
//! Contains Flag, Head, Pos and other common types.
//! 包含 Flag、Head、Pos 等公共类型。

mod flag;
mod head;
mod pos;

pub use flag::Flag;
pub use head::{
  CRC_SIZE, HEAD_CRC, HEAD_SIZE, HEAD_TOTAL, Head, HeadBuilder, INFILE_MAX, KEY_MAX, MAGIC,
};
pub use pos::Pos;
