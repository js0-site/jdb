//! GC lock / GC 锁
//!
//! Write lock on separate lock file, prevents concurrent GC on same WAL.
//! Readers can still read WAL file while GC holds this lock.
//! 在单独锁文件上加写锁，防止对同一 WAL 并发 GC。
//! GC 持有此锁时，读取者仍可读取 WAL 文件。

use std::{fs, path::PathBuf};

use crate::{Error, FileLock, Result};

/// GC lock on lock file / GC 锁文件锁
pub struct Lock {
  _lock: FileLock,
  path: PathBuf,
}

impl Drop for Lock {
  fn drop(&mut self) {
    let _ = fs::remove_file(&self.path);
  }
}

impl Lock {
  /// Try acquire GC lock / 尝试获取 GC 锁
  pub fn try_new(path: PathBuf) -> Result<Self> {
    if let Some(parent) = path.parent() {
      let _ = fs::create_dir_all(parent);
    }

    let file = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&path)
      .map_err(|_| Error::Locked)?;

    Ok(Self {
      _lock: FileLock::try_new(file)?,
      path,
    })
  }
}
