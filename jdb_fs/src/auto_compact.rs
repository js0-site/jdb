//! Auto-compacting log wrapper
//! 自动压缩日志包装器

use std::{
  io,
  io::Cursor,
  mem,
  path::{Path, PathBuf},
};

use compio::{
  fs::File,
  io::{AsyncReadAt, AsyncWrite, AsyncWriteExt, BufWriter},
};
use zbin::Bin;

use crate::{
  AtomWrite, Compact, Decoded, IncrCount, buf_writer_with_pos, consts::BUF_READ_SIZE,
  file::read_write, item::ParseResult,
};

/// Auto-compacting log wrapper
/// 自动压缩日志包装器
pub struct AutoCompact<T> {
  pub inner: T,
  pub file: BufWriter<Cursor<File>>,
  pub path: PathBuf,
  pub pos: u64,
  pub count: usize,
}

impl<T: Compact> AutoCompact<T> {
  /// Open and load from file
  /// 打开并从文件加载
  pub async fn open(mut inner: T, path: PathBuf) -> io::Result<Self> {
    let file = read_write(&path).await?;
    let (pos, count) = Self::load(&mut inner, &file).await?;
    let file = buf_writer_with_pos(file, pos);

    Ok(Self {
      inner,
      file,
      path,
      pos,
      count,
    })
  }

  /// Load data from file into inner, return (pos, count)
  /// 从文件加载数据到 inner，返回 (pos, count)
  async fn load(inner: &mut T, file: &File) -> io::Result<(u64, usize)> {
    let mut buf = Vec::with_capacity(BUF_READ_SIZE);
    let mut chunk = vec![0u8; BUF_READ_SIZE];
    let mut pos = 0u64;
    let mut file_pos = 0u64;
    let mut count = 0usize;

    loop {
      let result = file.read_at(chunk, file_pos).await;
      chunk = result.1;
      let n = result.0?;

      if n == 0 {
        break;
      }

      file_pos += n as u64;

      if buf.is_empty() {
        mem::swap(&mut buf, &mut chunk);
        buf.truncate(n);
        chunk.resize(BUF_READ_SIZE, 0);
      } else {
        buf.extend_from_slice(&chunk[..n]);
      }

      let mut offset = 0;
      while offset < buf.len() {
        match inner.parse(&buf[offset..]) {
          Some(decoded) => {
            offset += decoded.len;
            pos += decoded.len as u64;
            if decoded.incr {
              count += 1;
            }
          }
          None => break,
        }
      }

      if offset > 0 {
        if offset == buf.len() {
          buf.clear();
        } else {
          buf.drain(..offset);
        }
      }
    }

    Ok((pos, count))
  }

  /// Append single item (raw data, will be encoded)
  /// 追加单个条目（原始数据，会被编码）
  pub async fn push(&mut self, data: &[u8], incr: bool) -> io::Result<u64> {
    let start_pos = self.pos;
    if !data.is_empty() {
      let len = T::write_data(data, &mut self.file).await?;
      self.pos += len as u64;
      if incr {
        self.count += 1;
      }
    }
    self.file.flush().await?;
    Ok(start_pos)
  }

  /// Append multiple items (raw data, will be encoded)
  /// 追加多个条目（原始数据，会被编码）
  pub async fn push_iter<'a>(
    &mut self,
    iter: impl IntoIterator<Item = (&'a [u8], IncrCount)>,
  ) -> io::Result<u64> {
    let start_pos = self.pos;

    for (data, incr) in iter {
      if !data.is_empty() {
        let len = T::write_data(data, &mut self.file).await?;
        self.pos += len as u64;
        if incr {
          self.count += 1;
        }
      }
    }

    self.file.flush().await?;
    Ok(start_pos)
  }

  /// Check and compact if interval reached
  /// 检查并在达到间隔时压缩
  pub async fn maybe_compact(&mut self) -> io::Result<bool> {
    if self.count < T::INTERVAL {
      return Ok(false);
    }
    self.compact().await?;
    Ok(true)
  }

  /// Force compact
  /// 强制压缩
  pub async fn compact(&mut self) -> io::Result<()> {
    self.pos = rewrite(&self.path, &self.inner).await?;

    let file = read_write(&self.path).await?;
    self.file = buf_writer_with_pos(file, self.pos);

    self.count = 0;
    Ok(())
  }
}

/// Rewrite file from Compact rewrite iterator (streaming)
/// 从 Compact rewrite 迭代器重写文件（流式）
async fn rewrite<T: Compact>(path: &Path, inner: &T) -> io::Result<u64> {
  let mut file = AtomWrite::open(path.to_path_buf()).await?;
  let mut pos = 0u64;
  let mut has_content = false;

  for chunk in inner.rewrite() {
    let len = chunk.len();
    if len > 0 {
      file.write_all(chunk.io()).await.0?;
      pos += len as u64;
      has_content = true;
    }
  }

  if !has_content {
    let _ = compio::fs::remove_file(path).await;
    return Ok(0);
  }

  file.rename().await?;
  Ok(pos)
}
