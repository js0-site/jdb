//! Write lock for current WAL file / 当前 WAL 文件写锁
//!
//! Exclusive lock directly on WAL file, prevents multiple writers.
//! 直接在 WAL 文件上加排他锁，防止多个写入者。

use std::{fs, path::Path};

use crate::{Error, FileLock, Result, WalLock};

/// Write lock on WAL file / WAL 文件写锁
#[derive(Default)]
pub struct Lock(Option<FileLock>);

impl WalLock for Lock {
  fn try_lock(&mut self, path: &Path) -> Result<()> {
    let file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .open(path)
      .map_err(|_| Error::Locked)?;

    self.0 = Some(FileLock::try_new(file)?);
    Ok(())
  }
}
