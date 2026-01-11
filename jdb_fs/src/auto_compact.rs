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
use zbin::Bin;

use crate::{
  AtomWrite, Compact, IncrCount, Result, Size, atom_write::TMP, buf_writer_with_pos, item::write,
  load, push_iter, read_write,
};

/// Auto-compacting log wrapper
/// 自动压缩日志包装器
pub struct AutoCompact<T: Compact> {
  pub inner: T,
  fs: Option<BufWriter<Cursor<File>>>,
  pub path: PathBuf,
  pub pos: u64,
  pub count: usize,
}

impl<T: Compact> AutoCompact<T>
where
  T::Head: 'static,
{
  /// Open and load from fs
  /// 打开并从文件加载
  pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
    let path = path.into();
    let mut inner = T::default();
    let mut count = 0;
    let load = load::open::<T>(&path, |head, _| {
      if inner.on_head(head) {
        count += 1;
      }
    })
    .await?;
    let fs = buf_writer_with_pos(load.fs, load.pos);

    Ok(Self {
      inner,
      fs: Some(fs),
      path,
      pos: load.pos,
      count,
    })
  }

  #[inline]
  fn writer(&mut self) -> &mut BufWriter<Cursor<File>> {
    // Safe: fs is always Some except during compact
    // 安全：fs 除了 compact 期间总是 Some
    unsafe { self.fs.as_mut().unwrap_unchecked() }
  }

  /// Append single item with data, return bytes written
  /// 追加单个条目和数据，返回写入字节数
  pub async fn push<'a>(&mut self, head: T::Head, data: impl Bin<'a>, incr: bool) -> Result<Size> {
    let len = write::<T>(head, data, self.writer()).await?;
    self.pos += len;
    if incr {
      self.count += 1;
    }
    self.writer().flush().await?;
    Ok(len)
  }

  /// Append multiple items with data, return bytes written
  /// 追加多个条目和数据，返回写入字节数
  pub async fn push_iter<'a, D: Bin<'a>>(
    &mut self,
    iter: impl IntoIterator<Item = (T::Head, D, IncrCount)>,
  ) -> Result<Size> {
    let mut count = 0usize;
    let len = push_iter::<T, _>(
      iter.into_iter().map(|(head, data, incr)| {
        if incr {
          count += 1;
        }
        (head, data)
      }),
      self.writer(),
    )
    .await?;
    self.pos += len;
    self.count += count;
    self.writer().flush().await?;
    Ok(len)
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
    if let Some(mut w) = self.fs.take() {
      w.flush().await?;
      drop(w.into_inner().into_inner());
    }

    // Rewrite to new fs
    // 重写到新文件
    self.pos = rewrite(&self.path, &self.inner).await?;

    // Re-open fs at correct position
    // 在正确位置重新打开文件
    let fs = read_write(&self.path).await?;
    self.fs = Some(buf_writer_with_pos(fs, self.pos));
    self.count = 0;
    Ok(())
  }
}

/// Rewrite fs from Compact rewrite iterator
/// 从 Compact rewrite 迭代器重写文件
async fn rewrite<T: Compact>(path: &Path, inner: &T) -> Result<u64>
where
  T::Head: 'static,
{
  let mut fs = AtomWrite::open(path).await?;
  let mut pos = 0u64;
  let mut has_content = false;

  for head in inner.rewrite() {
    let len = write::<T>(*head, &[], &mut *fs).await?;
    pos += len;
    has_content = true;
  }

  if !has_content {
    drop(fs);
    let _ = compio::fs::remove_file(add_ext(path, TMP)).await;
    return Ok(0);
  }

  fs.rename().await?;
  Ok(pos)
}
