//! Tests for try_rm module
//! try_rm 模块测试

use std::fs;

use aok::{OK, Void};
use jdb_fs::try_rm;
use tempfile::tempdir;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_try_rm_unlocked() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("unlocked.tmp");

  fs::write(&path, b"test").unwrap();
  assert!(path.exists());

  assert!(try_rm(&path));
  assert!(!path.exists());
}

#[test]
fn test_try_rm_nonexistent() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("nonexistent.tmp");

  assert!(!try_rm(&path));
}

#[test]
fn test_try_rm_locked() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("locked.tmp");

  fs::write(&path, b"test").unwrap();
  let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
  file.try_lock().unwrap();

  assert!(!try_rm(&path));
  assert!(path.exists());

  drop(file);

  assert!(try_rm(&path));
  assert!(!path.exists());
}

#[compio::test]
async fn test_try_rm_after_atom_write_drop() -> Void {
  use compio::io::AsyncWriteExt;
  use jdb_fs::AtomWrite;

  let dir = tempdir()?;
  let path = dir.path().join("atom_drop.txt");
  let tmp = path.with_extension("txt.tmp");

  {
    let mut writer = AtomWrite::open(&path).await?;
    writer.write_all(b"test").await.0?;
  }

  assert!(!tmp.exists());

  OK
}
