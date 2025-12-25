//! Filesystem metadata operations
//! 文件系统元数据操作

use std::path::{Path, PathBuf};

use compio::runtime::spawn_blocking;

use crate::{Error, Result};

/// Run blocking IO 执行阻塞 IO
async fn blocking<T: Send + 'static>(
  f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T> {
  Ok(spawn_blocking(f).await.map_err(|_| Error::Join)??)
}

/// Atomic rename 原子重命名
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<()> {
  let from = from.as_ref().to_path_buf();
  let to = to.as_ref().to_path_buf();
  blocking(move || std::fs::rename(from, to)).await
}

/// Remove file 删除文件
pub async fn remove(path: impl AsRef<Path>) -> Result<()> {
  let path = path.as_ref().to_path_buf();
  blocking(move || std::fs::remove_file(path)).await
}

/// Create directory recursively 递归创建目录
pub async fn mkdir(path: impl AsRef<Path>) -> Result<()> {
  let path = path.as_ref().to_path_buf();
  blocking(move || std::fs::create_dir_all(path)).await
}

/// List file_li in directory (file_li only, no subdirs)
/// 列出目录中的文件（仅文件，不含子目录）
pub async fn ls(path: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
  let path = path.as_ref().to_path_buf();
  blocking(move || {
    let mut file_li = Vec::new();
    for entry in std::fs::read_dir(path)? {
      let entry = entry?;
      if entry.file_type()?.is_file() {
        file_li.push(entry.path());
      }
    }
    Ok(file_li)
  })
  .await
}

/// Check if path exists 检查路径是否存在
#[inline]
pub fn exists(path: impl AsRef<Path>) -> bool {
  path.as_ref().exists()
}

/// Get file size without opening 获取文件大小（无需打开）
pub async fn size(path: impl AsRef<Path>) -> Result<u64> {
  let path = path.as_ref().to_path_buf();
  blocking(move || Ok(std::fs::metadata(path)?.len())).await
}

/// Sync directory metadata (for WAL durability)
/// 同步目录元数据（用于 WAL 持久化）
#[cfg(unix)]
pub async fn sync_dir(path: impl AsRef<Path>) -> Result<()> {
  use std::os::unix::io::AsRawFd;
  let path = path.as_ref().to_path_buf();
  blocking(move || {
    let dir = std::fs::File::open(&path)?;
    // fsync on directory ensures metadata (directory entries) is persisted
    // 对目录 fsync 确保元数据（目录项）已持久化
    if unsafe { libc::fsync(dir.as_raw_fd()) } == -1 {
      return Err(std::io::Error::last_os_error());
    }
    Ok(())
  })
  .await
}

/// Sync directory metadata (no-op on Windows, NTFS auto-syncs metadata)
/// 同步目录元数据（Windows 上为空操作，NTFS 自动同步元数据）
#[cfg(windows)]
pub async fn sync_dir(_path: impl AsRef<Path>) -> Result<()> {
  Ok(())
}
