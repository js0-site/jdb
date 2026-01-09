//! File operations utilities
//! 文件操作工具

use std::path::Path;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::File;
use zbin::Bin;

/// Open file for reading
/// 打开文件用于读取
#[inline]
pub async fn open_read(path: impl AsRef<Path>) -> std::io::Result<File> {
  compio_fs::OpenOptions::new().read(true).open(path).await
}

/// Open file for reading and writing
/// 打开文件用于读写
#[inline]
pub async fn open_read_write(path: impl AsRef<Path>) -> std::io::Result<File> {
  compio_fs::OpenOptions::new()
    .read(true)
    .write(true)
    .open(path)
    .await
}

/// Open file for reading and writing, create if not exists
/// 打开文件用于读写，不存在则创建
#[inline]
pub async fn open_read_write_create(path: impl AsRef<Path>) -> std::io::Result<File> {
  compio_fs::OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(path)
    .await
}

/// Open file for writing, create if not exists
/// 打开文件用于写入，不存在则创建
#[inline]
pub async fn open_write_create(path: impl AsRef<Path>) -> std::io::Result<File> {
  compio_fs::OpenOptions::new()
    .write(true)
    .create(true)
    .open(path)
    .await
}

/// Write data to file (zero-copy for owned types)
/// 将数据写入文件（拥有所有权类型零拷贝）
#[inline]
pub async fn write_file<'a>(path: impl AsRef<Path>, data: impl Bin<'a>) -> std::io::Result<()> {
  let mut file = open_write_create(&path).await?;
  let buf = data.io();
  file.write_all_at(buf, 0).await.0
}

/// Atomic write: write to temp file, sync, then rename
/// 原子写入：写入临时文件，sync，然后重命名
pub async fn atomic_write(path: &Path, data: Vec<u8>) -> std::io::Result<u64> {
  let tmp = path.with_extension("tmp");
  let len = data.len() as u64;

  // Ensure temp file is removed on failure
  // 确保失败时删除临时文件
  defer_lite::defer! { let _ = std::fs::remove_file(&tmp); }

  let mut file = File::create(&tmp).await?;
  file.write_all_at(data, 0).await.0?;
  file.sync_all().await?;
  drop(file);

  compio::fs::rename(&tmp, path).await?;
  Ok(len)
}

/// Read entire file into Vec / 读取整个文件到 Vec
#[inline]
pub async fn read_all(file: &File, len: u64) -> std::io::Result<Vec<u8>> {
  if len == 0 {
    return Ok(Vec::new());
  }

  let buf = vec![0; len as usize];
  let slice = buf.slice(0..len as usize);
  let res = file.read_exact_at(slice, 0).await;
  res.0?;
  Ok(res.1.into_inner())
}
