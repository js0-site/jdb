//! Buffer utilities
//! 缓冲区工具

use std::io::Cursor;

use compio::{fs::File, io::BufWriter};

use crate::consts::BUF_WRITER_SIZE;

/// Create a BufWriter with cursor and default capacity
/// 使用游标和默认容量创建 BufWriter
pub fn buf_writer(file: File) -> BufWriter<Cursor<File>> {
  BufWriter::with_capacity(BUF_WRITER_SIZE, Cursor::new(file))
}

/// Create a BufWriter with cursor at specific position and default capacity
/// 使用游标（在特定位置）和默认容量创建 BufWriter
pub fn buf_writer_with_pos(file: File, pos: u64) -> BufWriter<Cursor<File>> {
  let mut cursor = Cursor::new(file);
  cursor.set_position(pos);
  BufWriter::with_capacity(BUF_WRITER_SIZE, cursor)
}
