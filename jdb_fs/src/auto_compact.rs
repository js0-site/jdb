//! Auto-compacting log wrapper
//! 自动压缩日志包装器

use std::{
  io::Cursor,
  path::{Path, PathBuf},
};

use compio::{
  fs::File,
  io::{AsyncWrite, BufWriter},
};
use zbin::Bin;

use crate::{
  buf_writer_with_pos, file::read_write, AtomWrite, Compact, CompactLoad, IncrCount, Load,
};

use crate::item::Result;

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
  pub async fn open(inner: T, path: PathBuf) -> Result<Self> {
    let file = read_write(&path).await?;
    let mut loader = CompactLoad::new(inner);
    let loaded = loader.load(&file).await?;
    let file = buf_writer_with_pos(file, loaded.pos);

    Ok(Self {
      inner: loader.inner,
      file,
      path,
      pos: loaded.pos,
      count: loaded.count,
    })
  }

  /// Append single item
  /// 追加单个条目
  pub async fn push(&mut self, data: &[u8], incr: bool) -> Result<u64> {
    let start_pos = self.pos;
    if !data.is_empty() {
      // Use zero-alloc write
      // 使用零分配写入
      let len = T::write_data(data, &mut self.file).await?;
      self.pos += len as u64;
      if incr {
        self.count += 1;
      }
    }
    self.file.flush().await?;
    Ok(start_pos)
  }

  /// Append multiple items
  /// 追加多个条目
  pub async fn push_iter<'a>(
    &mut self,
    iter: impl IntoIterator<Item = (&'a [u8], IncrCount)>,
  ) -> Result<u64> {
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
  pub async fn maybe_compact(&mut self) -> Result<bool> {
    if self.count < T::INTERVAL {
      return Ok(false);
    }
    self.compact().await?;
    Ok(true)
  }

  /// Force compact
  /// 强制压缩
  pub async fn compact(&mut self) -> Result<()> {
    self.pos = rewrite(&self.path, &self.inner).await?;

    let file = read_write(&self.path).await?;
    self.file = buf_writer_with_pos(file, self.pos);

    self.count = 0;
    Ok(())
  }
}

/// Rewrite file from Compact rewrite iterator
/// 从 Compact rewrite 迭代器重写文件
async fn rewrite<T: Compact>(path: &Path, inner: &T) -> Result<u64> {
  let mut file = AtomWrite::open(path).await?;
  let mut pos = 0u64;
  let mut has_content = false;

  for data in inner.rewrite() {
    let slice = data.as_slice();
    if !slice.is_empty() {
      // Direct write to AtomWrite (BufWriter)
      // 直接写入 AtomWrite (BufWriter)
      let len = T::write_data(slice, &mut *file).await?;
      pos += len as u64;
      has_content = true;
    }
  }

  if !has_content {
    // Drop file (AtomWrite) first to release lock/handle
    // 先释放 file (AtomWrite) 以释放锁/句柄
    drop(file);
    let _ = compio::fs::remove_file(path).await;
    return Ok(0);
  }

  file.rename().await?;
  Ok(pos)
}
