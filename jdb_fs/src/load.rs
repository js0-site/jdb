//! Load functions for loading data from file
//! 从文件加载数据的函数

use std::{mem::size_of, path::Path};

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  fs::File,
  io::AsyncReadAt,
};

use crate::{
  Pos,
  consts::BUF_READ_SIZE,
  item::{DataLen, Item, Offset, ParseResult, Result, Row, parse},
  read_write,
};

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
  on_head: impl FnMut(I::Head, Offset),
) -> Result<Load> {
  let file = read_write(path).await?;
  let pos = load::<I>(&file, on_head).await?;
  Ok(Load { pos, file })
}

/// Load data from reader
/// 从读取器加载数据
pub async fn load<I: Item>(
  reader: &impl AsyncReadAt,
  mut on_head: impl FnMut(I::Head, Offset),
) -> Result<Pos> {
  let row_size = size_of::<Row<I::Head>>();
  let mut buf = vec![0u8; BUF_READ_SIZE];
  let mut file_pos = 0u64;
  let mut buf_len = 0usize;
  let mut offset = 0usize;
  let mut pos = 0u64;

  // Read more data, return if EOF
  // 读取更多数据，EOF 时返回
  macro_rules! read_more {
    () => {{
      // Move remaining data to front
      // 将剩余数据移到前面
      if offset > 0 && offset < buf_len {
        buf.copy_within(offset..buf_len, 0);
      }
      buf_len -= offset;
      offset = 0;

      let BufResult(r, b) = reader.read_at(buf.slice(buf_len..), file_pos).await;
      buf = b.into_inner();
      let n = r?;
      if n == 0 {
        return Ok(pos);
      }
      file_pos += n as u64;
      buf_len += n;
    }};
  }

  // Initial read
  // 初始读取
  read_more!();

  loop {
    match parse::<I>(&buf[offset..buf_len]) {
      ParseResult::Ok(head) => {
        let data_len = head.data_len();
        let total_len = row_size + data_len;
        // data offset = pos + row_size
        // 数据偏移 = pos + row_size
        on_head(head, pos + row_size as u64);
        pos += total_len as u64;
        offset += total_len;
        if offset >= buf_len {
          read_more!();
        }
      }
      ParseResult::NeedMore => {
        let remain = buf_len - offset;
        // Remaining data fills buffer, need more space
        // 剩余数据占满缓冲区，需要更多空间
        if remain == buf.len() {
          buf.resize(buf.len() * 2, 0);
        }
        read_more!();
      }
      ParseResult::Err(e, skip) => {
        log::warn!("Load error at {pos}: {e}, skip {skip}");
        pos += skip as u64;
        offset += skip;
        if offset >= buf_len {
          read_more!();
        }
      }
    }
  }
}
