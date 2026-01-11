//! Load functions for loading data from file
//! 从文件加载数据的函数

use std::{mem::size_of, path::Path};

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  fs::File,
  io::AsyncReadAt,
};

use crate::{
  consts::BUF_READ_SIZE,
  item::{DataLen, Item, Offset, ParseResult, Result, Row, parse},
  read_write,
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
  on_data: impl FnMut(I::Head, Offset),
) -> Result<Load> {
  let file = read_write(path).await?;
  let pos = load::<I>(&file, on_data).await?;
  Ok(Load { pos, file })
}

/// Load data from reader
/// 从读取器加载数据
pub async fn load<I: Item>(
  reader: &impl AsyncReadAt,
  mut on_data: impl FnMut(I::Head, Offset),
) -> Result<Pos> {
  let row_size = size_of::<Row<I::Head>>();
  let mut buf = vec![0u8; BUF_READ_SIZE];
  let mut file_pos = 0u64;
  let mut buf_len = 0usize;
  let mut pos = 0u64;

  loop {
    // Read more data into buffer
    // 读取更多数据到缓冲区
    let BufResult(r, b) = reader.read_at(buf.slice(buf_len..), file_pos).await;
    buf = b.into_inner();
    let n = r?;
    if n == 0 && buf_len == 0 {
      break;
    }
    file_pos += n as u64;
    buf_len += n;

    let mut offset = 0usize;

    while offset < buf_len {
      match parse::<I>(&buf[offset..buf_len]) {
        ParseResult::Ok(head) => {
          let data_len = head.data_len();
          let total_len = row_size + data_len;
          // data offset = pos + row_size
          // data 偏移 = pos + row_size
          on_data(head, pos as Offset + row_size);
          pos += total_len as u64;
          offset += total_len;
        }
        ParseResult::NeedMore => {
          // EOF with incomplete data
          // EOF 时数据不完整
          if n == 0 {
            offset = buf_len;
          }
          break;
        }
        ParseResult::Err(e, skip) => {
          log::warn!("Load error at {pos}: {e}, skip {skip}");
          pos += skip as u64;
          offset += skip;
        }
      }
    }

    // If we haven't consumed anything and buffer is full, we need more space
    // 如果未消耗任何数据且缓冲区已满，则需要更多空间
    if offset == 0 && buf_len == buf.len() {
      buf.resize(buf.len() * 2, 0);
    }

    // Move remaining data to front
    // 将剩余数据移到前面
    if offset > 0 && offset < buf_len {
      buf.copy_within(offset..buf_len, 0);
    }
    buf_len -= offset;

    if n == 0 {
      break;
    }
  }

  Ok(pos)
}
