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

use crate::{AtomWrite, Compact, IncrCount, Load, buf_writer_with_pos, item::Result, read_write};

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
  pub async fn open(path: PathBuf) -> Result<Self> {
    let mut inner = T::default();
    let mut count = 0;
    let load = Load::open::<T>(&path, |data| {
      if inner.on_item(data) {
        count += 1;
      }
    })
    .await?;
    let file = buf_writer_with_pos(load.file, load.pos);

    Ok(Self {
      inner,
      file,
      path,
      pos: load.pos,
      count,
    })
  }

  /// Append single item
  /// 追加单个条目
  pub async fn push(&mut self, data: T::Data<'_>, incr: bool) -> Result<u64> {
    let start_pos = self.pos;
    let slice = data.as_slice();
    if !slice.is_empty() {
      // Use zero-alloc write
      // 使用零分配写入
      let len = T::write_data(slice, &mut self.file).await?;
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
    iter: impl IntoIterator<Item = (T::Data<'a>, IncrCount)>,
  ) -> Result<u64>
  where
    T: 'a,
  {
    let start_pos = self.pos;

    for (data, incr) in iter {
      let slice = data.as_slice();
      if !slice.is_empty() {
        let len = T::write_data(slice, &mut self.file).await?;
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
    // Safe to ignore error if file not exists
    // 如果文件不存在，忽略错误是安全的
    let _ = compio::fs::remove_file(crate::add_ext(path, crate::atom_write::TMP)).await;
    return Ok(0);
  }

  file.rename().await?;
  Ok(pos)
}
