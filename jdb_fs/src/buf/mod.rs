//! Buffer utilities
//! 缓冲区工具

mod mod_fn {
  use std::io::Cursor;

  use compio::{fs::File, io::BufWriter};

  use crate::consts::BUF_WRITER_SIZE;

  /// Create a BufWriter with cursor and default capacity
  /// 使用游标和默认容量创建 BufWriter
  pub fn buf_writer(fs: File) -> BufWriter<Cursor<File>> {
    BufWriter::with_capacity(BUF_WRITER_SIZE, Cursor::new(fs))
  }

  /// Create a BufWriter with cursor at specific position
  /// 使用游标（在特定位置）创建 BufWriter
  pub fn buf_writer_with_pos(fs: File, pos: u64) -> BufWriter<Cursor<File>> {
    let mut cursor = Cursor::new(fs);
    cursor.set_position(pos);
    BufWriter::with_capacity(BUF_WRITER_SIZE, cursor)
  }
}

pub use mod_fn::*;

mod file;
pub use file::File as BufFile;
