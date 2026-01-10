//! Load struct for loading data from file
//! 从文件加载数据的结构体

use std::{io, io::Cursor, path::Path};

use compio::{
  buf::IntoInner,
  fs::File,
  io::{AsyncBufRead, BufReader},
};

use crate::{
  buf_reader,
  file::read_write,
  item::{Item, ParseResult, parse},
};

/// Load struct for loading data from file
/// 从文件加载数据的结构体
pub struct Load {
  pub pos: u64,
  pub file: File,
}

impl Load {
  /// Open file and load data
  /// 打开文件并加载数据
  pub async fn open<I: Item>(
    path: impl AsRef<Path>,
    decode: impl for<'a> Fn(&'a [u8]) -> Option<I::Data<'a>>,
    mut on_data: impl FnMut(I::Data<'_>),
  ) -> io::Result<Self> {
    let file = read_write(path).await?;
    let mut reader = buf_reader(file);
    let pos = load::<I>(&mut reader, &decode, &mut on_data).await?;
    let file = reader.into_inner().into_inner();
    Ok(Self { pos, file })
  }
}

/// Load data from reader
/// 从读取器加载数据
async fn load<I: Item>(
  reader: &mut BufReader<Cursor<File>>,
  decode: &impl for<'a> Fn(&'a [u8]) -> Option<I::Data<'a>>,
  on_data: &mut impl FnMut(I::Data<'_>),
) -> io::Result<u64> {
  let mut pos = 0u64;

  loop {
    let buf = reader.fill_buf().await?;
    if buf.is_empty() {
      break;
    }

    let mut offset = 0;
    while offset < buf.len() {
      let slice = &buf[offset..];
      let len = match parse::<I, _>(slice, decode) {
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

  Ok(pos)
}
