//! Auto-compacting log wrapper
//! 自动压缩日志包装器

use std::{io, path::PathBuf};

use compio::{
  fs::File,
  io::{AsyncWriteAtExt, AsyncWriteExt},
};
use zbin::Bin;

use crate::fs::open_read_write;

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
#[expect(async_fn_in_trait, reason = "compio single-threaded runtime")]
pub trait Compact {
  /// Iterator item type for compaction
  /// 压缩迭代器项类型
  type Item: for<'a> zbin::Bin<'a>;

  /// Current entry count (for threshold check)
  /// 当前条目数（用于阈值检查）
  fn count(&self) -> usize;

  /// Expected count after compaction
  /// 压缩后的预期条目数
  fn compact_count(&self) -> usize;

  /// Iterate entries for rewrite
  /// 迭代条目用于重写
  fn iter(&self) -> impl Iterator<Item = Self::Item>;

  /// Rewrite file (atomic)
  /// 重写文件（原子）
  async fn rewrite(&self, path: &std::path::Path) -> io::Result<u64> {
    let iter = self.iter();
    let mut file = crate::AtomWrite::new(path.to_path_buf()).await?;
    let mut pos = 0u64;
    let mut has_content = false;

    for chunk in iter {
      if !chunk.is_empty() {
        let len = chunk.len();
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
}

/// Auto-compacting log wrapper
/// 自动压缩日志包装器
pub struct AutoCompact<T, const THRESHOLD: usize> {
  pub inner: T,
  pub file: File,
  pub path: PathBuf,
  pub pos: u64,
  pub count: usize,
}

impl<T: Compact, const THRESHOLD: usize> AutoCompact<T, THRESHOLD> {
  /// Create from inner and path (rewrite on load if needed)
  /// 从内部数据和路径创建（如需要则在加载时重写）
  pub async fn new(inner: T, path: PathBuf) -> io::Result<Self> {
    let count = inner.count();
    let compact_count = inner.compact_count();

    // Rewrite on load if has garbage
    // 如有垃圾则在加载时重写
    let pos = if count > compact_count {
      inner.rewrite(&path).await?
    } else {
      compio::fs::metadata(&path)
        .await
        .map(|m| m.len())
        .unwrap_or(0)
    };

    let file = open_read_write(&path).await?;
    let count = compact_count;

    Ok(Self {
      inner,
      file,
      path,
      pos,
      count,
    })
  }

  /// Append data and increment count
  /// 追加数据并增加计数
  pub async fn append(&mut self, data: impl zbin::Bin<'_>) -> io::Result<u64> {
    let len = data.len();
    self.file.write_all_at(data.io(), self.pos).await.0?;
    self.file.sync_all().await?;
    let pos = self.pos;
    self.pos += len as u64;
    self.count += 1;
    Ok(pos)
  }

  /// Append multiple items (single sync)
  /// 追加多个项（单次 sync）
  pub async fn append_many<I, B>(&mut self, items: I) -> io::Result<u64>
  where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
  {
    let mut buf = Vec::new();
    let mut n = 0usize;
    for item in items {
      buf.extend_from_slice(item.as_ref());
      n += 1;
    }

    if buf.is_empty() {
      return Ok(self.pos);
    }

    let len = buf.len();
    self.file.write_all_at(buf, self.pos).await.0?;
    self.file.sync_all().await?;
    let pos = self.pos;
    self.pos += len as u64;
    self.count += n;
    Ok(pos)
  }

  /// Check and compact if threshold exceeded
  /// 检查并在超过阈值时压缩
  pub async fn maybe_compact(&mut self) -> io::Result<bool> {
    if self.count < THRESHOLD {
      return Ok(false);
    }
    self.compact().await?;
    Ok(true)
  }

  /// Force compact
  /// 强制压缩
  pub async fn compact(&mut self) -> io::Result<()> {
    self.pos = self.inner.rewrite(&self.path).await?;
    self.file = open_read_write(&self.path).await?;
    self.count = self.inner.compact_count();
    Ok(())
  }
}
