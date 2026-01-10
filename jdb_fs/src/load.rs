//! Load functions for loading data from file
//! 从文件加载数据的函数

use std::{io, io::Cursor, path::Path};

use compio::{
  buf::IntoInner,
  fs::File,
  io::{AsyncBufRead, AsyncReadAt, BufReader},
};

use crate::{
  consts::BUF_READ_SIZE,
  file::read_write,
  item::{Item, ParseResult, parse},
};

pub type Pos = u64;
/// Load result
/// 加载结果
pub struct Load {
  pub pos: Pos,
  pub file: File,
}

/// Open file and load data
/// 打开文件并加载数据
pub async fn open<I: Item>(
  path: impl AsRef<Path>,
  decode: impl for<'a> Fn(&'a [u8]) -> Option<I::Data<'a>>,
  on_data: impl FnMut(I::Data<'_>),
) -> io::Result<Load> {
  let file = read_write(path).await?;
  let (pos, file) = load::<I, _>(file, decode, on_data).await?;
  Ok(Load { pos, file })
}

/// Load data from reader
/// 从读取器加载数据
pub async fn load<I: Item, ReadAt: AsyncReadAt>(
  reader_at: ReadAt,
  decode: impl for<'a> Fn(&'a [u8]) -> Option<I::Data<'a>>,
  mut on_data: impl FnMut(I::Data<'_>),
) -> io::Result<(Pos, ReadAt)> {
  let mut reader = BufReader::with_capacity(BUF_READ_SIZE, Cursor::new(reader_at));
  let mut pos = 0u64;

  loop {
    let buf = reader.fill_buf().await?;
    if buf.is_empty() {
      break;
    }

    let mut offset = 0;
    while offset < buf.len() {
      let slice = &buf[offset..];
      let len = match parse::<I, _>(slice, &decode) {
        ParseResult::Ok(data, len) => {
          on_data(data);
          pos += len as u64;
          len
        }
        ParseResult::NeedMore => 0,
        ParseResult::Err(e, skip) => {
          log::warn!("Load {e}, skipping {skip}");
          pos += skip as u64;
          skip
        }
      };
      if len == 0 {
        break;
      }
      offset += len;
    }

    if offset == 0 {
      // Need more data but buffer is full, can't proceed
      // 需要更多数据但缓冲区已满，无法继续
      break;
    }
    reader.consume(offset);
  }

  let file = reader.into_inner().into_inner();
  Ok((pos, file))
}
