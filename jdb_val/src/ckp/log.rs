//! Checkpoint log - simple single-file WAL
//! 检查点日志 - 简单的单文件 WAL

use std::path::PathBuf;

use compio::io::AsyncWriteAtExt;
use compio_fs::File;
use jdb_base::Load;

use super::entry::{self, CkpEntry, HEADER_SIZE};
use crate::Result;

use jdb_base::read_all;

/// Checkpoint log
/// 检查点日志
pub struct Log {
  path: PathBuf,
  file: Option<File>,
  pos: u64,
}

impl Log {
  #[inline]
  pub fn new(path: impl Into<PathBuf>) -> Self {
    Self {
      path: path.into(),
      file: None,
      pos: 0,
    }
  }

  pub async fn open(&mut self) -> Result<()> {
    if let Some(parent) = self.path.parent() {
      std::fs::create_dir_all(parent)?;
    }

    let file = compio_fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&self.path)
      .await?;

    let len = file.metadata().await?.len();
    // Recover valid end position using Load trait
    // 使用 Load trait 恢复有效结束位置
    self.pos = CkpEntry::recover(&file, 0, len).await;
    self.file = Some(file);
    Ok(())
  }

  /// Append entry
  /// 追加条目
  pub async fn append(&mut self, data: &[u8]) -> Result<u64> {
    let file = self.file.as_mut().ok_or(crate::Error::NotOpen)?;
    let start = self.pos;

    let buf = entry::build(data);
    let len = buf.len();
    file.write_all_at(buf, start).await.0?;
    self.pos = start + len as u64;

    Ok(start)
  }

  pub async fn sync(&self) -> Result<()> {
    if let Some(file) = &self.file {
      file.sync_all().await?;
    }
    Ok(())
  }

  /// Read all data
  /// 读取所有数据
  pub async fn read_all(&self) -> Result<Vec<u8>> {
    let file = self.file.as_ref().ok_or(crate::Error::NotOpen)?;
    Ok(read_all(file, self.pos).await?)
  }
}

/// Entry iterator
/// 条目迭代器
pub struct Iter<'a> {
  buf: &'a [u8],
  pos: usize,
}

impl<'a> Iter<'a> {
  #[inline]
  pub fn new(buf: &'a [u8]) -> Self {
    Self { buf, pos: 0 }
  }
}

impl<'a> Iterator for Iter<'a> {
  /// (offset, data)
  type Item = (usize, &'a [u8]);

  fn next(&mut self) -> Option<Self::Item> {
    while self.pos + HEADER_SIZE <= self.buf.len() {
      let start = self.pos;
      if let Some((data, next)) = entry::parse(self.buf, self.pos) {
        self.pos = next;
        return Some((start, data));
      }
      // Skip to find next magic
      // 跳过找下一个 magic
      self.pos += 1;
      if let Some(idx) = CkpEntry::find_magic(&self.buf[self.pos..]) {
        self.pos += idx;
      } else {
        break;
      }
    }
    None
  }
}
