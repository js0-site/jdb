//! Atomic write with rename
//! 原子写入并重命名
//!
//! Write to temp file, then rename to target on success
//! 写入临时文件，成功后重命名到目标

use std::{
  ops::{Deref, DerefMut},
  os::fd::{FromRawFd, IntoRawFd},
  path::{Path, PathBuf},
};

use compio::io::AsyncWriteAtExt;
use compio_fs::File;
use fs4::fs_std::FileExt;
use log::error;

/// Write to temp file, rename on success, delete on failure
/// 写入临时文件，成功时重命名，失败时删除
pub struct AtomWrite {
  file: File,
  tmp: PathBuf,
  dst: PathBuf,
  renamed: bool,
}

impl AtomWrite {
  /// Create temp file with exclusive lock (tmp = dst.tmp)
  /// 创建带排他锁的临时文件
  pub async fn new(dst: impl AsRef<Path>) -> std::io::Result<Self> {
    let dst = dst.as_ref();
    let tmp = dst.with_extension("tmp");
    // Create with std, lock, then convert to compio
    // 用 std 创建，加锁，然后转为 compio
    let std_file = std::fs::File::create(&tmp)?;
    std_file.lock_exclusive()?;
    let file = unsafe { File::from_raw_fd(std_file.into_raw_fd()) };
    Ok(Self {
      file,
      tmp,
      dst: dst.to_path_buf(),
      renamed: false,
    })
  }

  /// Sync and rename to destination
  /// 同步并重命名到目标
  pub async fn rename(mut self) -> std::io::Result<()> {
    self.file.sync_all().await?;
    compio::fs::rename(&self.tmp, &self.dst).await?;
    self.renamed = true;
    Ok(())
  }
}

impl Deref for AtomWrite {
  type Target = File;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.file
  }
}

impl DerefMut for AtomWrite {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.file
  }
}

impl Drop for AtomWrite {
  fn drop(&mut self) {
    if !self.renamed
      && let Err(e) = std::fs::remove_file(&self.tmp)
    {
      error!("remove tmp file failed: {}, err={e}", self.tmp.display());
    }
  }
}

/// Atomic write: write to temp file, sync, then rename
/// 原子写入：写入临时文件，sync，然后重命名
pub async fn atom_write(path: &Path, data: Vec<u8>) -> std::io::Result<u64> {
  let len = data.len() as u64;
  let mut file = AtomWrite::new(path).await?;
  file.write_all_at(data, 0).await.0?;
  file.rename().await?;
  Ok(len)
}

/// Check if tmp file can be deleted (not locked)
/// 检查临时文件是否可删除（未被锁定）
pub fn can_rm_tmp(path: &Path) -> bool {
  let Ok(file) = std::fs::File::open(path) else {
    return false;
  };
  file.try_lock_exclusive().is_ok()
}
