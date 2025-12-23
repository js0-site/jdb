#![cfg_attr(docsrs, feature(doc_cfg))]

//! 异步文件系统封装 Async file system wrapper

use std::io;
use std::path::Path;

use compio::fs::OpenOptions;
use compio::io::{AsyncReadAt, AsyncWriteAt};
use jdb_alloc::AlignedBuf;
use jdb_comm::PAGE_SIZE;

/// 异步文件 Async file
pub struct File {
  inner: compio::fs::File,
}

impl File {
  /// 只读打开 Open read-only
  pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
    let inner = OpenOptions::new().read(true).open(path).await?;
    Ok(Self { inner })
  }

  /// 创建新文件 Create new file
  pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
    let inner = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(path)
      .await?;
    Ok(Self { inner })
  }

  /// 读写打开 Open read-write
  pub async fn open_rw(path: impl AsRef<Path>) -> io::Result<Self> {
    let inner = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(path)
      .await?;
    Ok(Self { inner })
  }

  /// 指定偏移读取 Read at offset
  pub async fn read_at(&self, offset: u64, len: usize) -> io::Result<AlignedBuf> {
    let buf = AlignedBuf::with_cap(len);
    let compio::BufResult(result, buf) = self.inner.read_at(buf, offset).await;
    let n = result?;
    let mut buf = buf;
    unsafe { buf.set_len(n) };
    Ok(buf)
  }

  /// 读取单页 Read single page
  #[inline]
  pub async fn read_page(&self, page_no: u32) -> io::Result<AlignedBuf> {
    self.read_at(page_no as u64 * PAGE_SIZE as u64, PAGE_SIZE).await
  }

  /// 读取多页 Read multiple pages
  #[inline]
  pub async fn read_pages(&self, page_no: u32, count: u32) -> io::Result<AlignedBuf> {
    self
      .read_at(
        page_no as u64 * PAGE_SIZE as u64,
        count as usize * PAGE_SIZE,
      )
      .await
  }

  /// 指定偏移写入 Write at offset
  pub async fn write_at(&mut self, offset: u64, buf: AlignedBuf) -> io::Result<AlignedBuf> {
    let len = buf.len();
    let compio::BufResult(result, buf) = self.inner.write_at(buf, offset).await;
    let n = result?;
    if n != len {
      return Err(io::Error::new(
        io::ErrorKind::WriteZero,
        "incomplete write",
      ));
    }
    Ok(buf)
  }

  /// 写入单页 Write single page
  #[inline]
  pub async fn write_page(&mut self, page_no: u32, buf: AlignedBuf) -> io::Result<AlignedBuf> {
    self.write_at(page_no as u64 * PAGE_SIZE as u64, buf).await
  }

  /// 同步到磁盘 Sync to disk
  pub async fn sync(&self) -> io::Result<()> {
    self.inner.sync_all().await
  }

  /// 获取文件大小 Get file size
  pub async fn size(&self) -> io::Result<u64> {
    Ok(self.inner.metadata().await?.len())
  }
}
