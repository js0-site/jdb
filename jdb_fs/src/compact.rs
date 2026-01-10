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
  AtomWrite,
  consts::{BUF_READ_SIZE, BUF_WRITER_SIZE, COMPACT_INTERVAL},
  file::read_write,
};

/// Increment count flag
/// 增加计数标志
pub type IncrCount = bool;
pub type Offset = u64;

/// Decode result containing length and increment flag
/// 解码结果，包含长度和增量标志
#[derive(Debug, Clone, Copy)]
pub struct Decoded {
  pub len: usize,
  /// If entry exists, increment compact count
  /// 如果之前存在条目，表示需要增加压缩计数
  pub incr: bool,
}

/// Decode operation result
/// 解码操作结果
#[derive(Debug, Clone, Copy)]
pub enum DecodeResult {
  /// Successfully decoded, contains length and increment flag
  /// 解码成功，包含长度和增量标志
  Ok(Decoded),
  /// Need more bytes to complete decoding
  /// 需要更多字节才能完成解码
  NeedMore,
  /// End of valid data, subsequent writes should start from this offset
  /// 有效数据结束，后续写入应从此偏移量开始
  End(Offset),
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

  /// Decode single item and load into self
  /// 解码单条并加载到 self
  fn decode(&mut self, buf: &[u8]) -> io::Result<DecodeResult>;

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
    // Open with read/write/create at once to avoid race conditions and double syscalls
    // 一次性以读写创建模式打开，避免竞态条件和二次系统调用
    let file = read_write(&path).await?;
    let (pos, count) = Self::load(&mut inner, &file).await?;
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
  async fn load(inner: &mut T, file: &File) -> io::Result<(u64, usize)> {
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

      // Swap if buf is empty to avoid copy (Zero-Copy)
      // 如果 buf 为空，直接交换所有权以避免复制
      if buf.is_empty() {
        mem::swap(&mut buf, &mut chunk);
      } else {
        buf.extend_from_slice(&chunk);
      }

      // Decode items using offset to avoid frequent drain (O(N) memmove)
      // 使用偏移量解码条目，避免频繁 drain 导致的 O(N) 内存移动
      let mut offset = 0;
      let mut end_reached = false;
      loop {
        if offset >= buf.len() {
          break;
        }
        match inner.decode(&buf[offset..])? {
          DecodeResult::Ok(decoded) => {
            offset += decoded.len;
            pos += decoded.len as u64;
            if decoded.incr {
              count += 1;
            }
          }
          DecodeResult::NeedMore => break,
          DecodeResult::End(end_offset) => {
            pos += end_offset;
            end_reached = true;
            break;
          }
        }
      }

      // Remove processed bytes from buffer
      // 移除缓冲区中已处理的字节
      if offset > 0 {
        // clear is O(1) vs drain O(N)
        // 如果完全消费，使用 clear (O(1))，否则才用 drain (O(N))
        if offset == buf.len() {
          buf.clear();
        } else {
          buf.drain(..offset);
        }
      }

      if end_reached {
        break;
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

    // Re-open file for writing at new position
    // 在新位置重新打开文件以进行写入
    let file = read_write(&self.path).await?;
    let mut cursor = Cursor::new(file);
    cursor.set_position(self.pos);
    self.file = BufWriter::with_capacity(BUF_WRITER_SIZE, cursor);

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
