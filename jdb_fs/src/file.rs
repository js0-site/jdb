//! Simplified file operations
//! 简化的文件操作

use std::path::Path;

use compio::fs::OpenOptions;

/// Open file for reading and writing
/// 打开文件进行读写
pub async fn open_read_write(path: impl AsRef<Path>) -> std::io::Result<compio::fs::File> {
  OpenOptions::new().read(true).write(true).open(path).await
}

/// Open file for reading and writing, create if not exists
/// 打开文件进行读写，不存在则创建
pub async fn open_read_write_create(path: impl AsRef<Path>) -> std::io::Result<compio::fs::File> {
  OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(path)
    .await
}
