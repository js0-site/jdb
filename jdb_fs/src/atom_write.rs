//! Atomic write with rename
//! 原子写入并重命名
//!
//! Write to temp file, then rename to target on success.
//! 写入临时文件，成功后重命名到目标。

#[cfg(not(windows))]
use std::os::unix::io::FromRawFd;
#[cfg(not(windows))]
use std::os::unix::io::IntoRawFd;
#[cfg(windows)]
use std::os::windows::io::{FromRawHandle, IntoRawHandle};
use std::{
  fmt,
  fs::{self, OpenOptions},
  io::Cursor,
  ops::{Deref, DerefMut},
  path::{Path, PathBuf},
};

use compio::{
  buf::IntoInner,
  io::{AsyncWrite, BufWriter},
};
use compio_fs::File;
use log::error;

use crate::buf::buf_writer;

/// Default temporary extension
/// 默认临时扩展名
pub const TMP: &str = "tmp";

/// Write to temp file, rename on success, delete on failure
/// 写入临时文件，成功时重命名，失败时删除
pub struct AtomWrite {
  writer: Option<BufWriter<Cursor<File>>>,
  path: PathBuf,
  tmp: PathBuf,
}

impl AtomWrite {
  pub fn path(&self) -> &Path {
    &self.path
  }

  /// Create temp file with exclusive lock
  /// 创建带排他锁的临时文件
  pub async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
    let path = path.into();
    let tmp = crate::add_ext(&path, TMP);

    // Open with std::fs to get lock
    // 用 std::fs 打开以获取锁
    let std_file = OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp)?;

    std_file.try_lock()?;

    // Convert std File to compio File via raw fd/handle
    // 通过 raw fd/handle 将 std File 转换为 compio File
    #[cfg(not(windows))]
    let file = unsafe { File::from_raw_fd(std_file.into_raw_fd()) };

    #[cfg(windows)]
    let file = unsafe { File::from_raw_handle(std_file.into_raw_handle()) };

    let writer = buf_writer(file);

    Ok(Self {
      writer: Some(writer),
      path,
      tmp,
    })
  }

  /// Flush, sync and rename to destination
  /// 刷新、同步并重命名到目标
  pub async fn rename(mut self) -> std::io::Result<()> {
    if let Some(mut writer) = self.writer.take() {
      writer.flush().await?;
      let file = writer.into_inner().into_inner();
      file.sync_all().await?;

      // Drop file before rename
      // 重命名前释放文件
      drop(file);

      compio::fs::rename(&self.tmp, &self.path).await?;
    }
    Ok(())
  }
}

impl Deref for AtomWrite {
  type Target = BufWriter<Cursor<File>>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.writer.as_ref().unwrap()
  }
}

impl DerefMut for AtomWrite {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.writer.as_mut().unwrap()
  }
}

impl Drop for AtomWrite {
  fn drop(&mut self) {
    if let Some(file) = self.writer.take() {
      drop(file);
      if let Err(e) = fs::remove_file(&self.tmp) {
        error!("remove tmp file failed: {} {e}", self.tmp.display());
      }
    }
  }
}

impl fmt::Display for AtomWrite {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "AtomWrite {}", self.path.display())
  }
}
