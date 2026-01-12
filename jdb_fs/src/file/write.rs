//! AsyncWrite implementation for BufFile
//! BufFile 的 AsyncWrite 实现

use std::io;

use compio::{
  buf::{IoBuf, IoVectoredBuf},
  io::AsyncWrite,
};

use super::BufFile;

impl AsyncWrite for BufFile {
  async fn write<T: IoBuf>(&mut self, buf: T) -> compio::BufResult<usize, T> {
    let slice = buf.as_slice();
    let len = slice.len();
    if len == 0 {
      return compio::BufResult(Ok(0), buf);
    }

    self.wait_if_full().await;
    self.i().cur().push(self.pos, slice);
    self.pos += len as u64;
    self.spawn_flush();

    compio::BufResult(Ok(len), buf)
  }

  async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> compio::BufResult<usize, T> {
    let mut total = 0;
    for slice in buf.iter_slice() {
      if !slice.is_empty() {
        self.wait_if_full().await;
        self.i().cur().push(self.pos, slice);
        self.pos += slice.len() as u64;
        total += slice.len();
      }
    }
    if total > 0 {
      self.spawn_flush();
    }
    compio::BufResult(Ok(total), buf)
  }

  async fn flush(&mut self) -> io::Result<()> {
    BufFile::flush(self).await;
    Ok(())
  }

  async fn shutdown(&mut self) -> io::Result<()> {
    self.sync().await
  }
}
