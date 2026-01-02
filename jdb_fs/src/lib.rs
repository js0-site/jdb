#![cfg_attr(docsrs, feature(doc_cfg))]

//! jdb_fs - File operations for jdb
//! jdb 文件操作

mod file_lru;
mod fs;
mod fs_id;
mod head;
mod load;

pub use file_lru::FileLru;
pub use fs::{
  open_read, open_read_write, open_read_write_create, open_write_create, read_all, write_file,
};
pub use fs_id::{decode_id, encode_id, id_path};
pub use head::{
  CRC_SIZE, HEAD_CRC, HEAD_SIZE, HEAD_TOTAL, Head, HeadBuilder, HeadError, INFILE_MAX, KEY_MAX,
  MAGIC,
};
pub use load::{HeadEnd, INVALID, Load};
