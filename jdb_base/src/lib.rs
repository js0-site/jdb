//! jdb_base - Shared types for jdb
//! jdb 共享类型
//!
//! Contains Flag, Head, Pos, Load and other common types.
//! 包含 Flag、Head、Pos、Load 等公共类型。

mod file_lru;
mod flag;
mod fs;
mod fs_id;
mod head;
mod load;
mod pos;

pub use file_lru::FileLru;
pub use flag::Flag;
pub use fs::{open_read, open_read_write, open_read_write_create, open_write_create, read_all, write_file};
pub use fs_id::{decode_id, encode_id, id_path};
pub use head::{
  CRC_SIZE, HEAD_CRC, HEAD_SIZE, HEAD_TOTAL, Head, HeadBuilder, HeadError, INFILE_MAX, KEY_MAX,
  MAGIC,
};
pub use load::Load;
pub use pos::Pos;
