//! Async file with Direct I/O
//! 支持 Direct I/O 的异步文件

use std::path::Path;

#[cfg(unix)]
use std::os::fd::{AsRawFd, BorrowedFd};
#[cfg(windows)]
use compio::driver::ToSharedFd;
use compio::{
  buf::{IntoInner, IoBuf, IoBufMut},
  driver::op::{BufResultExt, ReadAt, WriteAt},
  fs::OpenOptions,
  runtime::submit,
};
use jdb_alloc::PAGE_SIZE;

use crate::{Error, Result, os};

const ALIGN_MASK: u64 = (PAGE_SIZE as u64) - 1;

/// Get fd for I/O operations 获取用于 I/O 操作的 fd
macro_rules! fd {
  ($self:expr) => {{
    #[cfg(unix)]
    {
      unsafe { BorrowedFd::borrow_raw($self.inner.as_raw_fd()) }
    }
    #[cfg(windows)]
    {
      $self.inner.to_shared_fd()
    }
  }};
}

#[inline(always)]
fn check_align(offset: u64, len: usize) -> Result<()> {
  if (offset | len as u64) & ALIGN_MASK != 0 {
    return Err(Error::Alignment {
      offset,
      len,
      align: PAGE_SIZE,
    });
  }
  Ok(())
}

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

  fn rw_opts(opts: &mut OpenOptions) {
    opts.read(true).write(true).create(true);
  }

  async fn with_opts(opts: OpenOptions, path: impl AsRef<Path>) -> Result<Self> {
    let inner = opts.open(path).await?;
    let file = Self { inner };
    #[cfg(unix)]
    os::post_open(file.inner.as_raw_fd())?;
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
    Self::rw_opts(&mut opts);
    opts.truncate(true);
    Self::with_opts(opts, path).await
  }

  /// Open read-write 读写打开
  pub async fn open_rw(path: impl AsRef<Path>) -> Result<Self> {
    let mut opts = Self::opts();
    Self::rw_opts(&mut opts);
    Self::with_opts(opts, path).await
  }

  /// Open for WAL 打开用于 WAL
  pub async fn open_wal(path: impl AsRef<Path>) -> Result<Self> {
    let mut opts = OpenOptions::new();
    os::dsync(&mut opts);
    Self::rw_opts(&mut opts);
    Self::with_opts(opts, path).await
  }

  /// Read at offset 在指定偏移读取
  #[inline(always)]
  pub async fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> Result<B> {
    let cap = buf.buf_capacity();
    check_align(offset, cap)?;

    let op = ReadAt::new(fd!(self), offset, buf);
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
    check_align(offset, len)?;

    let op = WriteAt::new(fd!(self), offset, buf);
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
    use std::os::fd::AsFd;

    let len = i64::try_from(len).map_err(|_| Error::Overflow(len))?;
    let owned = self.inner.as_fd().try_clone_to_owned()?;
    compio::runtime::spawn_blocking(move || os::preallocate(owned.as_raw_fd(), len))
      .await
      .map_err(|_| Error::Join)??;
    Ok(())
  }

  #[cfg(windows)]
  pub async fn preallocate(&self, len: u64) -> Result<()> {
    use std::os::windows::io::{AsHandle, OwnedHandle};

    let owned: OwnedHandle = self.inner.as_handle().try_clone_to_owned()?;
    let len = i64::try_from(len).map_err(|_| Error::Overflow(len))?;
    compio::runtime::spawn_blocking(move || os::preallocate(owned, len))
      .await
      .map_err(|_| Error::Join)??;
    Ok(())
  }
}
