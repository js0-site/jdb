//! Atomic write with rename
//! 原子写入并重命名
//!
//! Write to temp fs, then rename to target on success.
//! 写入临时文件，成功后重命名到目标。

#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawHandle, IntoRawHandle};
use std::{
  fmt,
  fs::{self, OpenOptions},
  io::Cursor,
  ops::{Deref, DerefMut},
  path::PathBuf,
};

use add_ext::add_ext;
use compio::{
  buf::IntoInner,
  io::{AsyncWrite, BufWriter},
};
use compio_fs::File;

use crate::buf::buf_writer;

/// Default temporary extension
/// 默认临时扩展名
pub const TMP: &str = "tmp";

/// Write to temp fs, rename on success, delete on failure
/// 写入临时文件，成功时重命名，失败时删除
pub struct AtomWrite {
  pub path: PathBuf,
  writer: Option<BufWriter<Cursor<File>>>,
  tmp: PathBuf,
}

impl AtomWrite {
  /// Create temp fs with exclusive lock
  /// 创建带排他锁的临时文件
  pub async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
    let path = path.into();
    let tmp = add_ext(&path, TMP);

    // Open with std::fs to get lock synchronously
    // 用 std::fs 同步打开以获取锁
    let std_fs = OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp)?;

    std_fs.try_lock()?;

    // Convert std File to compio File via raw fd/handle
    // 通过 raw fd/handle 将 std File 转换为 compio File
    #[cfg(unix)]
    let fs = unsafe { File::from_raw_fd(std_fs.into_raw_fd()) };

    #[cfg(windows)]
    let fs = unsafe { File::from_raw_handle(std_fs.into_raw_handle()) };

    let writer = buf_writer(fs);

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
      let fs = writer.into_inner().into_inner();
      fs.sync_all().await?;

      // Drop fs before rename (crucial for Windows)
      // 重命名前释放文件（对 Windows 至关重要）
      drop(fs);

      compio::fs::rename(&self.tmp, &self.path).await?;
    }
    Ok(())
  }
}

impl Deref for AtomWrite {
  type Target = BufWriter<Cursor<File>>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    // Safe: writer is only None after rename() or drop()
    // 安全：writer 仅在 rename() 或 drop() 后为 None
    unsafe { self.writer.as_ref().unwrap_unchecked() }
  }
}

impl DerefMut for AtomWrite {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    // Safe: writer is only None after rename() or drop()
    // 安全：writer 仅在 rename() 或 drop() 后为 None
    unsafe { self.writer.as_mut().unwrap_unchecked() }
  }
}

impl Drop for AtomWrite {
  #[cold]
  fn drop(&mut self) {
    if let Some(w) = self.writer.take() {
      drop(w);
      // Synchronous delete in Drop is a compromise for safety
      // Drop 中的同步删除是为了安全性的折衷
      if let Err(e) = fs::remove_file(&self.tmp)
        && e.kind() != std::io::ErrorKind::NotFound
      {
        log::error!("remove tmp fs failed: {} {e}", self.tmp.display());
      }
    }
  }
}

impl fmt::Display for AtomWrite {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "AtomWrite {}", self.path.display())
  }
}
