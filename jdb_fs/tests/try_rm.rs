//! Tests for try_rm module
//! try_rm 模块测试

use std::{fs, path::PathBuf};

use aok::{OK, Void};
use fs4::fs_std::FileExt;
use jdb_fs::try_rm;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

fn test_dir() -> PathBuf {
  let dir = PathBuf::from("target/test_try_rm");
  fs::create_dir_all(&dir).ok();
  dir
}

#[test]
fn test_try_rm_unlocked() {
  let dir = test_dir();
  let path = dir.join("unlocked.tmp");

  // Create file
  // 创建文件
  fs::write(&path, b"test").unwrap();
  assert!(path.exists());

  // Should delete successfully
  // 应成功删除
  assert!(try_rm(&path));
  assert!(!path.exists());
}

#[test]
fn test_try_rm_nonexistent() {
  let path = PathBuf::from("target/test_try_rm/nonexistent.tmp");

  // Should return false for nonexistent file
  // 不存在的文件应返回 false
  assert!(!try_rm(&path));
}

#[test]
fn test_try_rm_locked() {
  let dir = test_dir();
  let path = dir.join("locked.tmp");

  // Create and lock file
  // 创建并锁定文件
  fs::write(&path, b"test").unwrap();
  let file = fs::File::open(&path).unwrap();
  file.lock_exclusive().unwrap();

  // On macOS, same process can acquire lock again (advisory locks)
  // On Linux, try_rm should fail for locked file
  // macOS 上同进程可再次获取锁（建议锁）
  // Linux 上 try_rm 应无法删除锁定的文件
  #[cfg(target_os = "linux")]
  {
    assert!(!try_rm(&path));
    assert!(path.exists());
  }

  // Unlock and clean up
  // 解锁并清理
  drop(file);
  let _ = fs::remove_file(&path);
}

#[compio::test]
async fn test_try_rm_after_atom_write_drop() -> Void {
  use compio::io::AsyncWriteExt;
  use jdb_fs::AtomWrite;

  let dir = test_dir();
  let path = dir.join("atom_drop.txt");
  let tmp = path.with_extension("txt.tmp");

  let _ = fs::remove_file(&path);
  let _ = fs::remove_file(&tmp);

  {
    let mut writer = AtomWrite::open(&path, 4096).await?;
    writer.write_all(b"test").await.0?;
    // Drop cleans up tmp
    // Drop 会清理 tmp
  }

  // Tmp already cleaned by AtomWrite drop
  // tmp 已被 AtomWrite drop 清理
  assert!(!tmp.exists());

  OK
}
