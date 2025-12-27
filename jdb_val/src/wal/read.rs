//! WAL read operations / WAL 读取操作

use compio::io::AsyncReadAtExt;
use compio_fs::{File, OpenOptions};
use log::warn;
use zerocopy::FromBytes;

use super::{
  Wal,
  header::{HEADER_SIZE, HeaderState, check_header},
};
use crate::{
  Head, Pos,
  error::{Error, Result},
};

// Log messages / 日志消息
const LOG_SCAN_SMALL: &str = "WAL file too small for scan";
const LOG_SCAN_INVALID: &str = "WAL header invalid for scan";

impl Wal {
  /// Read head at location / 在位置读取头
  pub async fn read_head(&mut self, loc: Pos) -> Result<Head> {
    if let Some(head) = self.head_cache.get(&loc) {
      return Ok(*head);
    }

    let buf = vec![0u8; Head::SIZE];
    let res = if loc.id() == self.cur_id {
      let file = self.cur_file.as_ref().ok_or(Error::NotOpen)?;
      file.read_exact_at(buf, loc.pos()).await
    } else {
      let file = self.get_file(loc.id()).await?;
      file.read_exact_at(buf, loc.pos()).await
    };
    res.0?;
    let head = Head::read_from_bytes(&res.1).map_err(|_| Error::InvalidHead)?;
    self.head_cache.insert(loc, head);
    Ok(head)
  }

  pub(super) async fn get_file(&mut self, id: u64) -> Result<&File> {
    if !self.file_cache.contains_key(&id) {
      let path = self.wal_path(id);
      let file = OpenOptions::new().read(true).open(&path).await?;
      self.file_cache.insert(id, file);
    }
    Ok(self.file_cache.get(&id).unwrap())
  }

  /// Read data from WAL file / 从 WAL 文件读取数据
  pub async fn read_data(&mut self, loc: Pos, len: usize) -> Result<Vec<u8>> {
    if let Some(data) = self.data_cache.get(&loc) {
      return Ok(data.clone());
    }

    let buf = vec![0u8; len];
    let res = if loc.id() == self.cur_id {
      let file = self.cur_file.as_ref().ok_or(Error::NotOpen)?;
      file.read_exact_at(buf, loc.pos()).await
    } else {
      let file = self.get_file(loc.id()).await?;
      file.read_exact_at(buf, loc.pos()).await
    };
    res.0?;
    self.data_cache.insert(loc, res.1.clone());
    Ok(res.1)
  }

  /// Read data from separate file / 从独立文件读取数据
  pub async fn read_file(&self, id: u64) -> Result<Vec<u8>> {
    let path = self.bin_path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len() as usize;

    let buf = vec![0u8; len];
    let res = file.read_exact_at(buf, 0).await;
    res.0?;
    Ok(res.1)
  }

  /// Get key by head / 根据头获取键
  pub async fn head_key(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.key_flag.is_inline() {
      Ok(head.key_data().to_vec())
    } else {
      let loc = head.key_pos();
      if head.key_flag.is_infile() {
        let len = head.key_len.get() as usize;
        self.read_data(loc, len).await
      } else {
        self.read_file(loc.id()).await
      }
    }
  }

  /// Get val by head / 根据头获取值
  pub async fn head_val(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.val_flag.is_inline() {
      Ok(head.val_data().to_vec())
    } else {
      let loc = head.val_pos();
      let data = if head.val_flag.is_infile() {
        let len = head.val_len.get() as usize;
        self.read_data(loc, len).await?
      } else {
        self.read_file(loc.id()).await?
      };
      let crc = crc32fast::hash(&data);
      if crc != head.val_crc32() {
        return Err(Error::CrcMismatch(head.val_crc32(), crc));
      }
      Ok(data)
    }
  }

  /// Scan all entries in a WAL file / 扫描 WAL 文件中的所有条目
  pub async fn scan<F>(&self, id: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, &Head) -> bool,
  {
    let path = self.wal_path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len();

    if len < HEADER_SIZE as u64 {
      warn!("{LOG_SCAN_SMALL}: {path:?}");
      return Err(Error::InvalidHeader);
    }

    let mut buf = vec![0u8; HEADER_SIZE];
    let res = file.read_exact_at(buf, 0).await;
    res.0?;
    buf = res.1;
    if matches!(check_header(&mut buf), HeaderState::Invalid) {
      warn!("{LOG_SCAN_INVALID}: {path:?}");
      return Err(Error::InvalidHeader);
    }

    let mut pos = HEADER_SIZE as u64;
    let mut buf = vec![0u8; Head::SIZE];

    while pos + Head::SIZE as u64 <= len {
      let res = file.read_exact_at(buf, pos).await;
      res.0?;
      buf = res.1;

      let head = Head::read_from_bytes(&buf).map_err(|_| Error::InvalidHead)?;

      if !f(pos, &head) {
        break;
      }

      let data_len = if head.key_flag.is_infile() {
        head.key_len.get() as u64
      } else {
        0
      } + if head.val_flag.is_infile() {
        head.val_len.get() as u64
      } else {
        0
      };

      pos += Head::SIZE as u64 + data_len;
    }

    Ok(())
  }
}
