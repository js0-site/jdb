//! jdb_base - Shared types for jdb
//! jdb 共享类型
//!
//! Contains Flag, Head, Pos, Load and other common types.
//! 包含 Flag、Head、Pos、Load 等公共类型。

mod flag;
mod head;
mod load;
mod pos;

pub use flag::Flag;
pub use head::{
  CRC_SIZE, HEAD_CRC, HEAD_SIZE, HEAD_TOTAL, Head, HeadBuilder, HeadError, INFILE_MAX, KEY_MAX,
  MAGIC,
};
pub use load::Load;
pub use pos::Pos;
