//! Tests for atom_write module
//! atom_write 模块测试

use std::{fs, path::PathBuf};

use aok::{OK, Void};
use compio::io::AsyncWriteExt;
use jdb_fs::AtomWrite;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

fn test_dir() -> PathBuf {
  let dir = PathBuf::from("target/test_atom_write");
  fs::create_dir_all(&dir).ok();
  dir
}

#[compio::test]
async fn test_atom_write_success() -> Void {
  let dir = test_dir();
  let path = dir.join("success.txt");

  // Clean up
  // 清理
  let _ = fs::remove_file(&path);
  let tmp = path.with_extension("txt.tmp");
  let _ = fs::remove_file(&tmp);

  let content = b"hello world";
  let mut writer = AtomWrite::open(&path, 4096).await?;
  writer.write_all(content).await.0?;
  writer.rename().await?;

  // Verify file exists and content correct
  // 验证文件存在且内容正确
  assert!(path.exists());
  assert_eq!(fs::read(&path)?, content);

  // Tmp file should not exist
  // 临时文件不应存在
  assert!(!tmp.exists());

  // Clean up
  // 清理
  fs::remove_file(&path)?;
  OK
}

#[compio::test]
async fn test_atom_write_drop_cleanup() -> Void {
  let dir = test_dir();
  let path = dir.join("drop_test.txt");
  let tmp = path.with_extension("txt.tmp");

  // Clean up
  // 清理
  let _ = fs::remove_file(&path);
  let _ = fs::remove_file(&tmp);

  {
    let mut writer = AtomWrite::open(&path, 4096).await?;
    writer.write_all(b"test").await.0?;
    // Drop without rename
    // 不调用 rename 直接 drop
  }

  // Target should not exist
  // 目标文件不应存在
  assert!(!path.exists());

  // Tmp file should be cleaned up
  // 临时文件应被清理
  assert!(!tmp.exists());

  OK
}

#[compio::test]
async fn test_atom_write_path() -> Void {
  let dir = test_dir();
  let path = dir.join("path_test.txt");

  let _ = fs::remove_file(&path);
  let tmp = path.with_extension("txt.tmp");
  let _ = fs::remove_file(&tmp);

  let writer = AtomWrite::open(&path, 4096).await?;
  assert_eq!(writer.path(), &path);

  OK
}

#[compio::test]
async fn test_atom_write_large_content() -> Void {
  let dir = test_dir();
  let path = dir.join("large.txt");

  let _ = fs::remove_file(&path);
  let tmp = path.with_extension("txt.tmp");
  let _ = fs::remove_file(&tmp);

  // Write multiple times to test buffering
  // 多次写入以测试缓冲
  let mut writer = AtomWrite::open(&path, 1024).await?;

  // Write static data multiple times
  // 多次写入静态数据
  for _ in 0..100 {
    writer.write_all(b"0123456789").await.0?;
  }
  writer.rename().await?;

  let result = fs::read(&path)?;
  assert_eq!(result.len(), 1000);

  fs::remove_file(&path)?;
  OK
}
