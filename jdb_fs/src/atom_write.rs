//! Atomic write with rename
//! 原子写入并重命名
//!
//! Write to temp file, then rename to target on success
//! 写入临时文件，成功后重命名到目标

use std::{
  fs,
  io::Cursor,
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  os::fd::{AsRawFd, FromRawFd},
  path::{Path, PathBuf},
};

use compio::{
  buf::IntoInner,
  io::{AsyncWrite, BufWriter},
};
use compio_fs::File;
use fs4::fs_std::FileExt;
use log::error;

// Buffer size (64KB)
// 缓冲区大小
const BUF_SIZE: usize = 65536;

/// Write to temp file, rename on success, delete on failure
/// 写入临时文件，成功时重命名，失败时删除
pub struct AtomWrite {
  pub writer: Option<BufWriter<Cursor<File>>>,
  pub dst: PathBuf,
  pub renamed: bool,
}

impl AtomWrite {
  #[inline]
  fn tmp(&self) -> PathBuf {
    self.dst.with_extension("tmp")
  }

  /// Create temp file with exclusive lock (tmp = dst.tmp)
  /// 创建带排他锁的临时文件
  pub async fn new(dst: impl Into<PathBuf>) -> std::io::Result<Self> {
    let dst = dst.into();
    let tmp = dst.with_extension("tmp");

    let file = compio::fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp)
      .await?;

    // Use std::fs wrapper for flock (non-blocking try_lock)
    // Safety: ManuallyDrop ensures we don't close the fd when std_file drops
    // 使用 std::fs 包装器进行 flock（非阻塞 try_lock）
    // 安全：ManuallyDrop 确保 std_file drop 时不会关闭 fd
    let raw_fd = file.as_raw_fd();
    let std_file = unsafe { ManuallyDrop::new(fs::File::from_raw_fd(raw_fd)) };
    std_file.try_lock_exclusive()?;

    let writer = BufWriter::with_capacity(BUF_SIZE, Cursor::new(file));
    Ok(Self {
      writer: Some(writer),
      dst,
      renamed: false,
    })
  }

  /// Flush, sync and rename to destination
  /// 刷新、同步并重命名到目标
  pub async fn rename(mut self) -> std::io::Result<()> {
    let mut writer = self.writer.take().unwrap();
    writer.flush().await?;
    let file = writer.into_inner().into_inner();
    file.sync_all().await?;
    compio::fs::rename(self.tmp(), &self.dst).await?;
    self.renamed = true;
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
    if !self.renamed {
      let tmp = self.tmp();
      if let Err(e) = fs::remove_file(&tmp) {
        error!("remove tmp file failed: {}, err={e}", tmp.display());
      }
    }
  }
}

/// Try to delete tmp file if not locked, return true if deleted
/// 尝试删除未锁定的临时文件，删除成功返回 true
pub fn try_rm(path: &Path) -> bool {
  // Open with std::fs to check lock (sync operation, use with care in async context)
  // 使用 std::fs 打开以检查锁（同步操作，在异步上下文中需谨慎）
  if let Ok(file) = fs::File::open(path) {
    if file.try_lock_exclusive().is_ok() {
      drop(file);
      return fs::remove_file(path).is_ok();
    }
    false
  } else {
    false
  }
}
