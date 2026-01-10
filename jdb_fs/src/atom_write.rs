//! Atomic write with rename
//! 原子写入并重命名
//!
//! Write to temp file, then rename to target on success.
//! 写入临时文件，成功后重命名到目标。

use std::{
  fmt, fs,
  io::Cursor,
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  os::fd::{AsRawFd, FromRawFd},
  path::PathBuf,
};

use compio::{
  buf::IntoInner,
  io::{AsyncWrite, BufWriter},
};
use compio_fs::File;
use fs4::fs_std::FileExt;
use log::error;

/// Default temporary extension
/// 默认临时扩展名
pub const TMP: &str = "tmp";

/// Write to temp file, rename on success, delete on failure
/// 写入临时文件，成功时重命名，失败时删除
pub struct AtomWrite {
  /// Writer wrapped in Option to handle ownership in rename/drop
  /// 包装在 Option 中的 Writer，用于在 rename/drop 中处理所有权
  writer: Option<BufWriter<Cursor<File>>>,
  /// Target path
  /// 目标路径
  path: PathBuf,
  /// Temporary path (pre-calculated)
  /// 临时路径（预计算）
  tmp: PathBuf,
}

impl AtomWrite {
  /// Create temp file with exclusive lock
  /// 创建带排他锁的临时文件
  ///
  /// # Arguments
  /// * `path` - Target path / 目标路径
  /// * `buf_size` - Buffer buf_size / 缓冲区容量
  pub async fn new(path: impl Into<PathBuf>, buf_size: usize) -> std::io::Result<Self> {
    let path = path.into();
    // Optimization: calculate tmp path once
    // 优化：一次性计算临时路径
    let tmp = path.with_extension(TMP);

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

    // Compio BufWriter needs Cursor for Position-aware writing if implied,
    // though strictly File implements AsyncWriteAt. Kept as per original logic.
    // Compio BufWriter 需要 Cursor 进行位置感知的写入（如果隐含），
    // 尽管严格来说 File 实现了 AsyncWriteAt。保持原有逻辑。
    let writer = BufWriter::with_capacity(buf_size, Cursor::new(file));

    Ok(Self {
      writer: Some(writer),
      path,
      tmp,
    })
  }

  /// Flush, sync and rename to destination
  /// 刷新、同步并重命名到目标
  pub async fn rename(mut self) -> std::io::Result<()> {
    // Take writer out, effectively setting it to None (prevents Drop from deleting)
    // 取出 writer，有效地将其设为 None（防止 Drop 删除文件）
    if let Some(mut writer) = self.writer.take() {
      writer.flush().await?;
      let file = writer.into_inner().into_inner();

      file.sync_all().await?;

      // Windows: must drop file handle to release lock before rename
      // Windows: 必须 drop 文件句柄以释放锁，否则 rename 失败
      #[cfg(windows)]
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
    // Safety: writer is always Some unless rename() is called (which consumes self)
    // 安全：writer 始终为 Some，除非调用了 rename()（它消耗 self）
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
    // If writer is Some, rename was not called, so we clean up
    // 如果 writer 为 Some，说明未调用 rename，因此清理临时文件
    if self.writer.take().is_some() {
      // Use sync fs::remove_file as we cannot await in Drop.
      // Note: In single-threaded async, this blocks the runtime briefly.
      // 使用同步 fs::remove_file，因为 Drop 中无法 await。
      // 注意：在单线程异步中，这会短暂阻塞运行时。
      if let Err(e) = fs::remove_file(&self.tmp) {
        error!("remove tmp file failed: {}, err={e}", self.tmp.display());
      }
    }
  }
}

impl fmt::Display for AtomWrite {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.path.display())
  }
}
