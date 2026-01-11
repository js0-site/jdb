//! Auto-compacting log wrapper
//! 自动压缩日志包装器

use std::{
  io::Cursor,
  path::{Path, PathBuf},
};

use add_ext::add_ext;
use compio::{
  buf::IntoInner,
  fs::File,
  io::{AsyncWrite, BufWriter},
};

use crate::{
  AtomWrite, Compact, IncrCount, Result, atom_write::TMP, buf_writer_with_pos, item::write, load,
  read_write,
};

/// Auto-compacting log wrapper
/// 自动压缩日志包装器
pub struct AutoCompact<T: Compact> {
  pub inner: T,
  file: Option<BufWriter<Cursor<File>>>,
  pub path: PathBuf,
  pub pos: u64,
  pub count: usize,
}

impl<T: Compact> AutoCompact<T> {
  /// Open and load from file
  /// 打开并从文件加载
  pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
    let path = path.into();
    let mut inner = T::default();
    let mut count = 0;
    let load = load::open::<T>(&path, |data, _| {
      if inner.on_data(data) {
        count += 1;
      }
    })
    .await?;
    let file = buf_writer_with_pos(load.file, load.pos);

    Ok(Self {
      inner,
      file: Some(file),
      path,
      pos: load.pos,
      count,
    })
  }

  #[inline]
  fn writer(&mut self) -> &mut BufWriter<Cursor<File>> {
    // Safe: file is always Some except during compact
    // 安全：file 除了 compact 期间总是 Some
    unsafe { self.file.as_mut().unwrap_unchecked() }
  }

  /// Append single item
  /// 追加单个条目
  pub async fn push(&mut self, data: T::Head, incr: bool) -> Result<u64> {
    let start_pos = self.pos;
    let len = write::<T>(data, &[], self.writer()).await?;
    self.pos += len as u64;
    if incr {
      self.count += 1;
    }
    self.writer().flush().await?;
    Ok(start_pos)
  }

  /// Append multiple items
  /// 追加多个条目
  pub async fn push_iter(
    &mut self,
    iter: impl IntoIterator<Item = (T::Head, IncrCount)>,
  ) -> Result<u64> {
    let start_pos = self.pos;
    for (data, incr) in iter {
      let len = write::<T>(data, &[], self.writer()).await?;
      self.pos += len as u64;
      if incr {
        self.count += 1;
      }
    }
    self.writer().flush().await?;
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
    if let Some(mut w) = self.file.take() {
      w.flush().await?;
      drop(w.into_inner().into_inner());
    }

    // Rewrite to new file
    // 重写到新文件
    self.pos = rewrite(&self.path, &self.inner).await?;

    // Re-open file at correct position
    // 在正确位置重新打开文件
    let file = read_write(&self.path).await?;
    self.file = Some(buf_writer_with_pos(file, self.pos));
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
    let len = write::<T>(*data, &[], &mut *file).await?;
    pos += len as u64;
    has_content = true;
  }

  if !has_content {
    drop(file);
    let _ = compio::fs::remove_file(add_ext(path, TMP)).await;
    return Ok(0);
  }

  file.rename().await?;
  Ok(pos)
}
