//! WAL (Write-Ahead Log) implementation / WAL 预写日志实现
//!
//! Single-threaded async with compio / 基于 compio 的单线程异步

use std::fs;
use std::path::PathBuf;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};
use zerocopy::{FromBytes, IntoBytes};

use crate::error::{Error, Result};
use crate::{Head, Loc};

/// WAL file extension / WAL 文件扩展名
const EXT: &str = "wal";

/// WAL manager / WAL 管理器
pub struct Wal {
  dir: PathBuf,
  cur_id: u64,
  cur_file: Option<File>,
  cur_pos: u64,
  max_size: u64,
}

impl Wal {
  /// Create WAL manager / 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, max_size: u64) -> Self {
    Self {
      dir: dir.into(),
      cur_id: 0,
      cur_file: None,
      cur_pos: 0,
      max_size,
    }
  }

  /// Get file path by id / 根据 id 获取文件路径
  #[inline]
  fn path(&self, id: u64) -> PathBuf {
    let name = idpath::encode(&self.dir.to_string_lossy(), id);
    PathBuf::from(format!("{name}.{EXT}"))
  }

  /// Open or create current WAL file / 打开或创建当前 WAL 文件
  pub async fn open(&mut self) -> Result<()> {
    // Ensure dir exists / 确保目录存在
    fs::create_dir_all(&self.dir)?;

    self.cur_id = self.iter().max().unwrap_or(0);
    let path = self.path(self.cur_id);

    // Ensure parent dir exists / 确保父目录存在
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    let meta = file.metadata().await?;
    self.cur_pos = meta.len();
    self.cur_file = Some(file);
    Ok(())
  }

  /// Rotate to new WAL file / 轮转到新 WAL 文件
  async fn rotate(&mut self) -> Result<()> {
    self.cur_id += 1;
    let path = self.path(self.cur_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    self.cur_file = Some(file);
    self.cur_pos = 0;
    Ok(())
  }

  /// Write head + data, return location / 写入 head + data，返回位置
  pub async fn write(&mut self, head: &Head, data: &[u8]) -> Result<Loc> {
    // Check if need rotate / 检查是否需要轮转
    let total = Head::SIZE as u64 + data.len() as u64;
    if self.cur_pos + total > self.max_size {
      self.rotate().await?;
    }

    let file = self.cur_file.as_mut().ok_or(Error::NotOpen)?;
    let pos = self.cur_pos;

    // Write head / 写入头
    file.write_all_at(head.as_bytes().to_vec(), pos).await.0?;

    // Write data / 写入数据
    if !data.is_empty() {
      file
        .write_all_at(data.to_vec(), pos + Head::SIZE as u64)
        .await
        .0?;
    }

    self.cur_pos += total;
    Ok(Loc::new(self.cur_id, pos))
  }

  /// Read head at location / 在位置读取头
  pub async fn read_head(&self, loc: Loc) -> Result<Head> {
    let file = if loc.id() == self.cur_id {
      self.cur_file.as_ref().ok_or(Error::NotOpen)?
    } else {
      return Err(Error::FileNotFound(loc.id()));
    };

    let buf = vec![0u8; Head::SIZE];
    let res = file.read_exact_at(buf, loc.pos()).await;
    res.0?;

    Head::read_from_bytes(&res.1).map_err(|_| Error::InvalidHead)
  }

  /// Scan all entries in a WAL file / 扫描 WAL 文件中的所有条目
  pub async fn scan<F>(&self, id: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, &Head) -> bool,
  {
    let path = self.path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len();

    let mut pos = 0u64;
    let mut buf = vec![0u8; Head::SIZE];

    while pos + Head::SIZE as u64 <= len {
      let res = file.read_exact_at(buf, pos).await;
      res.0?;
      buf = res.1;

      let head = Head::read_from_bytes(&buf).map_err(|_| Error::InvalidHead)?;

      if !f(pos, &head) {
        break;
      }

      // Skip to next entry / 跳到下一条目
      let data_len = if head.key_flag.is_inline() {
        0
      } else {
        head.key_len.get() as u64
      } + if head.val_flag.is_inline() {
        0
      } else {
        head.val_len.get() as u64
      };

      pos += Head::SIZE as u64 + data_len;
    }

    Ok(())
  }

  /// Iterate all WAL file ids / 迭代所有 WAL 文件 id
  pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
    fs::read_dir(&self.dir)
      .into_iter()
      .flatten()
      .flatten()
      .filter_map(|entry| {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == EXT) {
          let stem = path.file_stem()?.to_str()?;
          let parent = path.parent().unwrap_or(&self.dir);
          let full = parent.join(stem);
          idpath::decode(full.to_string_lossy()).ok()
        } else {
          None
        }
      })
  }

  /// Remove WAL file (for GC) / 删除 WAL 文件（用于垃圾回收）
  pub fn remove(&self, id: u64) -> Result<()> {
    if id == self.cur_id {
      return Err(Error::CannotRemoveCurrent);
    }
    let path = self.path(id);
    fs::remove_file(&path)?;
    Ok(())
  }

  /// Sync current file / 同步当前文件
  pub async fn sync(&self) -> Result<()> {
    if let Some(file) = &self.cur_file {
      file.sync_all().await?;
    }
    Ok(())
  }

  /// Current file id / 当前文件 id
  #[inline]
  pub fn cur_id(&self) -> u64 {
    self.cur_id
  }

  /// Current position / 当前位置
  #[inline]
  pub fn cur_pos(&self) -> u64 {
    self.cur_pos
  }
}
