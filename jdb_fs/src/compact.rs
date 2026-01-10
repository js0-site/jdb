//! Auto-compacting log wrapper
//! 自动压缩日志包装器

use std::{
  io,
  io::Cursor,
  path::{Path, PathBuf},
};

use compio::{
  fs::{File, OpenOptions},
  io::{AsyncReadAt, AsyncWrite, AsyncWriteExt, BufWriter},
};
use zbin::Bin;

use crate::{
  AtomWrite,
  consts::{BUF_READ_SIZE, BUF_WRITER_SIZE, COMPACT_INTERVAL},
  file::{open_read_write, open_read_write_create},
};

/// Increment count flag
/// 增加计数标志
pub type IncrCount = bool;

/// Decode result
/// 解码结果
pub struct Decoded {
  pub len: usize,
  /// If entry exists, increment compact count
  /// 如果之前存在条目，表示需要增加压缩计数
  pub count: bool,
}

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
pub trait Compact: Sized {
  /// Compact operation interval (operations per compaction)
  /// 压缩操作间隔（每次压缩的操作次数）
  const INTERVAL: usize = COMPACT_INTERVAL;

  /// Read buffer size
  /// 读取缓冲区大小
  const BUF_READ_SIZE: usize = BUF_READ_SIZE;

  type Item<'a>: Bin<'a>
  where
    Self: 'a;

  /// Decode single item and load into self, return Decoded (len=0 means need more data)
  /// 解码单条并加载到 self，返回 Decoded（len=0 表示需要更多数据）
  fn decode(&mut self, buf: &[u8]) -> io::Result<Decoded>;

  /// Iterate bytes for full rewrite
  /// 迭代字节用于完全重写
  fn rewrite(&self) -> impl Iterator<Item = Self::Item<'_>>;
}

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
    let (pos, count) = match OpenOptions::new().read(true).open(&path).await {
      Ok(file) => Self::load(&mut inner, file).await?,
      Err(e) if e.kind() == io::ErrorKind::NotFound => (0, 0),
      Err(e) => return Err(e),
    };

    let file = open_read_write_create(&path).await?;
    let mut cursor = Cursor::new(file);
    cursor.set_position(pos);
    let file = BufWriter::with_capacity(BUF_WRITER_SIZE, cursor);

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
  async fn load(inner: &mut T, file: File) -> io::Result<(u64, usize)> {
    // Reuse buffer to reduce allocation
    // 复用缓冲区以减少分配
    let mut buf = Vec::with_capacity(T::BUF_READ_SIZE);
    let mut chunk = Vec::with_capacity(T::BUF_READ_SIZE);
    let mut pos = 0u64;
    let mut file_pos = 0u64;
    let mut count = 0usize;

    loop {
      // Prepare chunk buffer for reading
      // 准备读取缓冲区
      if chunk.len() < T::BUF_READ_SIZE {
        chunk.resize(T::BUF_READ_SIZE, 0);
      }

      let result = file.read_at(chunk, file_pos).await;
      chunk = result.1;
      let n = result.0?;

      if n == 0 {
        break;
      }

      file_pos += n as u64;
      chunk.truncate(n);
      buf.extend_from_slice(&chunk);

      // Decode items using offset to avoid frequent drain (O(N) memmove)
      // 使用偏移量解码条目，避免频繁 drain 导致的 O(N) 内存移动
      let mut offset = 0;
      loop {
        if offset >= buf.len() {
          break;
        }
        let decoded = inner.decode(&buf[offset..])?;
        if decoded.len == 0 {
          break;
        }
        offset += decoded.len;
        pos += decoded.len as u64;
        if decoded.count {
          count += 1;
        }
      }

      // Remove processed bytes from buffer
      // 移除缓冲区中已处理的字节
      if offset > 0 {
        buf.drain(..offset);
      }
    }

    Ok((pos, count))
  }

  /// Append single item
  /// 追加单个条目
  pub async fn push<'a>(&mut self, item: T::Item<'a>, incr: bool) -> io::Result<u64>
  where
    T: 'a,
  {
    self.push_iter([(item, incr)]).await
  }

  /// Append multiple items
  /// 追加多个条目
  pub async fn push_iter<'a>(
    &mut self,
    iter: impl IntoIterator<Item = (T::Item<'a>, IncrCount)>,
  ) -> io::Result<u64>
  where
    T: 'a,
  {
    let start_pos = self.pos;

    for (item, incr) in iter {
      let len = item.len();
      if len > 0 {
        self.file.write_all(item.io()).await.0?;
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
    if self.pos > 0 {
      let file = open_read_write(&self.path).await?;
      let mut cursor = Cursor::new(file);
      cursor.set_position(self.pos);
      self.file = BufWriter::with_capacity(BUF_WRITER_SIZE, cursor);
    }
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
