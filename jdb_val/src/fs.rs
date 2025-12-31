//! Path utilities for WAL file naming and file operations
//! WAL 文件命名的路径工具和文件操作

use std::path::{Path, PathBuf};

use compio::io::AsyncWriteAtExt;
use compio_fs::File;
use fast32::base32::CROCKFORD_LOWER;

/// Encode id to base32 string
/// 将 id 编码为 base32 字符串
#[inline(always)]
pub fn encode_id(id: u64) -> String {
  CROCKFORD_LOWER.encode_u64(id)
}

/// Decode base32 string to id
/// 将 base32 字符串解码为 id
#[inline(always)]
pub fn decode_id(name: &str) -> Option<u64> {
  CROCKFORD_LOWER.decode_u64(name.as_bytes()).ok()
}

/// Join dir with encoded id
/// 将目录与编码后的 id 拼接
#[inline(always)]
pub fn id_path(dir: &Path, id: u64) -> PathBuf {
  dir.join(encode_id(id))
}

/// Open file for reading
/// 打开文件用于读取
#[inline]
pub async fn open_read(path: impl AsRef<Path>) -> Result<File, std::io::Error> {
  compio_fs::OpenOptions::new().read(true).open(path).await
}

/// Open file for reading and writing
/// 打开文件用于读写
#[inline]
pub async fn open_read_write(path: impl AsRef<Path>) -> Result<File, std::io::Error> {
  compio_fs::OpenOptions::new()
    .read(true)
    .write(true)
    .open(path)
    .await
}

/// Open file for reading and writing, create if not exists
/// 打开文件用于读写，不存在则创建
#[inline]
pub async fn open_read_write_create(path: impl AsRef<Path>) -> Result<File, std::io::Error> {
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
pub async fn open_write_create(path: impl AsRef<Path>) -> Result<File, std::io::Error> {
  compio_fs::OpenOptions::new()
    .write(true)
    .create(true)
    .open(path)
    .await
}

/// Write data to file at offset 0
/// 将数据写入文件（偏移 0）
#[inline]
pub async fn write_file(path: impl AsRef<Path>, data: &[u8]) -> Result<(), std::io::Error> {
  let mut file = open_write_create(&path).await?;
  file.write_all_at(data.to_vec(), 0).await.0
}
