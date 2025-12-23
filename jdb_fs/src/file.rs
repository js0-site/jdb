//! Async file operations 异步文件操作

use compio::fs::OpenOptions;
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use jdb_alloc::AlignedBuf;
use jdb_comm::{JdbError, JdbResult, PAGE_SIZE};
use std::path::Path;

/// Async file wrapper 异步文件封装
pub struct File {
  inner: compio::fs::File,
}

impl File {
  /// Open for read 打开读取
  pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self> {
    let inner = OpenOptions::new().read(true).open(path).await?;
    Ok(Self { inner })
  }

  /// Create or truncate 创建或截断
  pub async fn create(path: impl AsRef<Path>) -> JdbResult<Self> {
    let inner = OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(path)
      .await?;
    Ok(Self { inner })
  }

  /// Open for read/write 打开读写
  pub async fn open_rw(path: impl AsRef<Path>) -> JdbResult<Self> {
    let inner = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(path)
      .await?;
    Ok(Self { inner })
  }

  /// Read at offset (reads full page, returns requested len)
  /// 在偏移处读取（读取整页，返回请求长度）
  pub async fn read_at(&self, offset: u64, len: usize) -> JdbResult<AlignedBuf> {
    // Round up to page boundary 向上取整到页边界
    let aligned_len = ((len + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    let mut buf = AlignedBuf::with_cap(aligned_len);
    unsafe { buf.set_len(aligned_len) };

    let res = self.inner.read_exact_at(buf, offset).await;
    res.0?;
    let mut buf = res.1;

    // Truncate to requested length 截断到请求长度
    unsafe { buf.set_len(len) };
    Ok(buf)
  }

  /// Read page at page number 读取指定页号的页
  pub async fn read_page(&self, page_no: u32) -> JdbResult<AlignedBuf> {
    self.read_at(u64::from(page_no) * PAGE_SIZE as u64, PAGE_SIZE).await
  }

  /// Write at offset 在偏移处写入
  pub async fn write_at(&mut self, offset: u64, buf: AlignedBuf) -> JdbResult<AlignedBuf> {
    let res = self.inner.write_all_at(buf, offset).await;
    res.0?;
    Ok(res.1)
  }

  /// Write page at page number 写入指定页号的页
  pub async fn write_page(&mut self, page_no: u32, buf: AlignedBuf) -> JdbResult<AlignedBuf> {
    if buf.len() != PAGE_SIZE {
      return Err(JdbError::PageSizeMismatch {
        expected: PAGE_SIZE,
        actual: buf.len(),
      });
    }
    self.write_at(u64::from(page_no) * PAGE_SIZE as u64, buf).await
  }

  /// Sync to disk 同步到磁盘
  pub async fn sync(&mut self) -> JdbResult<()> {
    self.inner.sync_all().await?;
    Ok(())
  }

  /// Get file size 获取文件大小
  pub async fn size(&self) -> JdbResult<u64> {
    let meta = self.inner.metadata().await?;
    Ok(meta.len())
  }
}
