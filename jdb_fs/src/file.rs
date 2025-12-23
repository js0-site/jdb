//! Async file with Direct I/O
//! 支持 Direct I/O 的异步文件

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, BorrowedHandle};
use std::path::Path;

use compio::{
  buf::{IntoInner, IoBuf, IoBufMut},
  driver::op::{BufResultExt, ReadAt, WriteAt},
  fs::OpenOptions,
  runtime::submit,
};
use jdb_alloc::PAGE_SIZE;

use crate::{
  error::{Error, Result},
  os,
};

const ALIGN_MASK: u64 = (PAGE_SIZE as u64) - 1;

/// Async file wrapper 异步文件封装
pub struct File {
  inner: compio::fs::File,
}

impl File {
  fn opts() -> OpenOptions {
    let mut opts = OpenOptions::new();
    os::direct(&mut opts);
    opts
  }

  async fn with_opts(opts: OpenOptions, path: impl AsRef<Path>) -> Result<Self> {
    let inner = opts.open(path).await?;
    let file = Self { inner };
    #[cfg(unix)]
    os::post_open(file.inner.as_raw_fd())?;
    #[cfg(windows)]
    os::post_open(file.inner.as_raw_handle())?;
    Ok(file)
  }

  /// Open read-only 只读打开
  pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
    let mut opts = Self::opts();
    opts.read(true);
    Self::with_opts(opts, path).await
  }

  /// Create new file (truncate) 创建新文件（截断）
  pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
    let mut opts = Self::opts();
    opts.read(true).write(true).create(true).truncate(true);
    Self::with_opts(opts, path).await
  }

  /// Open read-write 读写打开
  pub async fn open_rw(path: impl AsRef<Path>) -> Result<Self> {
    let mut opts = Self::opts();
    opts.read(true).write(true).create(true);
    Self::with_opts(opts, path).await
  }

  /// Open for WAL 打开用于 WAL
  pub async fn open_wal(path: impl AsRef<Path>) -> Result<Self> {
    let mut opts = OpenOptions::new();
    os::dsync(&mut opts);
    opts.read(true).write(true).create(true);
    Self::with_opts(opts, path).await
  }

  /// Read at offset 在指定偏移读取
  #[inline(always)]
  pub async fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> Result<B> {
    let cap = buf.buf_capacity();
    if (offset | cap as u64) & ALIGN_MASK != 0 {
      return Err(Error::Alignment {
        offset,
        len: cap,
        align: PAGE_SIZE,
      });
    }

    #[cfg(unix)]
    let fd = unsafe { BorrowedFd::borrow_raw(self.inner.as_raw_fd()) };
    #[cfg(windows)]
    let fd = unsafe { BorrowedHandle::borrow_raw(self.inner.as_raw_handle()) };

    let op = ReadAt::new(fd, offset, buf);
    let compio::BufResult(r, buf) = submit(op).await.into_inner().map_advanced();
    let n = r?;

    if n != cap {
      return Err(Error::ShortRead {
        expected: cap,
        actual: n,
      });
    }
    Ok(buf)
  }

  /// Write at offset 在指定偏移写入
  #[inline(always)]
  pub async fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> Result<B> {
    let len = buf.buf_len();
    if (offset | len as u64) & ALIGN_MASK != 0 {
      return Err(Error::Alignment {
        offset,
        len,
        align: PAGE_SIZE,
      });
    }

    #[cfg(unix)]
    let fd = unsafe { BorrowedFd::borrow_raw(self.inner.as_raw_fd()) };
    #[cfg(windows)]
    let fd = unsafe { BorrowedHandle::borrow_raw(self.inner.as_raw_handle()) };

    let op = WriteAt::new(fd, offset, buf);
    let compio::BufResult(r, buf) = submit(op).await.into_inner();
    let n = r?;

    if n != len {
      return Err(Error::ShortWrite {
        expected: len,
        actual: n,
      });
    }
    Ok(buf)
  }

  #[inline]
  pub async fn size(&self) -> Result<u64> {
    Ok(self.inner.metadata().await?.len())
  }

  pub async fn sync_all(&self) -> Result<()> {
    self.inner.sync_all().await?;
    Ok(())
  }

  pub async fn sync_data(&self) -> Result<()> {
    self.inner.sync_data().await?;
    Ok(())
  }

  /// Preallocate space 预分配空间
  #[cfg(unix)]
  pub async fn preallocate(&self, len: u64) -> Result<()> {
    let len = i64::try_from(len).map_err(|_| Error::Overflow(len))?;
    // OwnedFd auto-closes on drop, panic-safe
    // OwnedFd 自动关闭，panic 安全
    let owned = self.inner.as_fd().try_clone_to_owned()?;
    compio::runtime::spawn_blocking(move || os::preallocate(owned.as_raw_fd(), len))
      .await
      .map_err(|_| Error::Join)??;
    Ok(())
  }

  #[cfg(windows)]
  pub async fn preallocate(&self, len: u64) -> Result<()> {
    self.inner.set_len(len).await?;
    Ok(())
  }
}
