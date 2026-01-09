//! Auto-compacting log wrapper
//! 自动压缩日志包装器

use std::{io, io::Cursor, path::PathBuf};

use compio::{
  fs::File,
  io::{AsyncWrite, AsyncWriteExt, BufWriter},
};

use crate::fs::{open_read_write, open_read_write_create};

// Buffer size (64KB)
// 缓冲区大小
const BUF_SIZE: usize = 65536;

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
pub trait Compact {
  /// Expected count after compaction
  /// 压缩后的预期条目数
  fn compact_len(&self) -> usize;

  /// Iterate bytes for rewrite
  /// 迭代字节用于重写
  fn iter(&self) -> impl Iterator<Item = impl zbin::Bin<'_>>;
}

/// Auto-compacting log wrapper
/// 自动压缩日志包装器
pub struct AutoCompact<T> {
  pub inner: T,
  pub file: BufWriter<Cursor<File>>,
  pub path: PathBuf,
  pub pos: u64,
  pub count: usize,
  pub threshold: usize,
}

impl<T: Compact> AutoCompact<T> {
  /// Create from inner and path (rewrite on load if needed)
  /// 从内部数据和路径创建（如需要则在加载时重写）
  pub async fn new(inner: T, path: PathBuf, count: usize, threshold: usize) -> io::Result<Self> {
    let compact_len = inner.compact_len();

    // Rewrite on load if has garbage
    // 如有垃圾则在加载时重写
    let pos = if count > compact_len {
      rewrite(&path, &inner).await?
    } else {
      compio::fs::metadata(&path)
        .await
        .map(|m| m.len())
        .unwrap_or(0)
    };

    let file = open_read_write_create(&path).await?;
    let file = BufWriter::with_capacity(BUF_SIZE, Cursor::new(file));

    Ok(Self {
      inner,
      file,
      path,
      pos,
      count: compact_len,
      threshold,
    })
  }

  /// Append single item and increment count
  /// 追加单个条目并增加计数
  pub async fn push<'a>(&mut self, data: impl zbin::Bin<'a>) -> io::Result<u64> {
    self.save_iter([data]).await
  }

  /// Append multiple items and increment count
  /// 追加多个条目并增加计数
  pub async fn save_iter<'a>(
    &mut self,
    data: impl IntoIterator<Item = impl zbin::Bin<'a>>,
  ) -> io::Result<u64> {
    let start_pos = self.pos;
    let mut n = 0usize;

    for item in data {
      let len = item.len();
      if len > 0 {
        self.file.write_all(item.io()).await.0?;
        self.pos += len as u64;
        n += 1;
      }
    }

    self.file.flush().await?;
    self.count += n;
    Ok(start_pos)
  }

  /// Check and compact if threshold exceeded
  /// 检查并在超过阈值时压缩
  pub async fn maybe_compact(&mut self) -> io::Result<bool> {
    if self.count < self.threshold {
      return Ok(false);
    }
    self.compact().await?;
    Ok(true)
  }

  /// Force compact
  /// 强制压缩
  pub async fn compact(&mut self) -> io::Result<()> {
    self.pos = rewrite(&self.path, &self.inner).await?;
    let file = open_read_write(&self.path).await?;
    self.file = BufWriter::with_capacity(BUF_SIZE, Cursor::new(file));
    self.count = self.inner.compact_len();
    Ok(())
  }
}

/// Rewrite file from Compact iterator (streaming)
/// 从 Compact 迭代器重写文件（流式）
async fn rewrite<T: Compact>(path: &PathBuf, inner: &T) -> io::Result<u64> {
  use zbin::Bin;

  let mut file = crate::AtomWrite::new(path.clone()).await?;
  let mut pos = 0u64;
  let mut has_content = false;

  for chunk in inner.iter() {
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
