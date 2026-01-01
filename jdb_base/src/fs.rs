//! File operations utilities
//! 文件操作工具

use std::path::Path;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::File;

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

/// Write data to file at offset 0
/// 将数据写入文件（偏移 0）
#[inline]
pub async fn write_file(path: impl AsRef<Path>, data: &[u8]) -> std::io::Result<()> {
  let mut file = open_write_create(&path).await?;
  file.write_all_at(data.to_vec(), 0).await.0
}

/// Read entire file into Vec / 读取整个文件到 Vec

#[inline]

pub async fn read_all(file: &File, len: u64) -> std::io::Result<Vec<u8>> {
  if len == 0 {
    return Ok(Vec::new());
  }

  // Use MaybeUninit to safely handle uninitialized memory

  let mut buf: Vec<std::mem::MaybeUninit<u8>> = Vec::with_capacity(len as usize);

  unsafe { buf.set_len(len as usize) };

  let slice = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, len as usize) };

  let res = file.read_exact_at(slice, 0).await;

  res.0?;

  // Safety: read_exact_at has successfully initialized all bytes

  Ok(unsafe { std::mem::transmute::<Vec<std::mem::MaybeUninit<u8>>, Vec<u8>>(buf) })
}
