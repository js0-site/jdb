//! WAL read operations / WAL 读取操作

use std::rc::Rc;

use compio::io::AsyncReadAtExt;
use compio_fs::{File, OpenOptions};
use log::warn;
use zerocopy::FromBytes;

use super::{
  CachedData, Wal,
  header::{HEADER_SIZE, HeaderState, check_header},
};
use crate::{
  Head, Pos,
  error::{Error, Result},
};

// Log messages / 日志消息
const LOG_SCAN_SMALL: &str = "WAL file too small for scan";
const LOG_SCAN_INVALID: &str = "WAL header invalid for scan";
/// Scan buffer size (64KB) / 扫描缓冲区大小
const SCAN_BUF_SIZE: usize = 64 * 1024;

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
    // SAFETY: inserted above if not exists / 上面已插入
    Ok(unsafe { self.file_cache.get(&id).unwrap_unchecked() })
  }

  /// Read data from WAL file / 从 WAL 文件读取数据
  pub async fn read_data(&mut self, loc: Pos, len: usize) -> Result<CachedData> {
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
    let data: CachedData = Rc::new(res.1.into_boxed_slice());
    self.data_cache.insert(loc, data.clone());
    Ok(data)
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
        Ok(self.read_data(loc, len).await?.to_vec())
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
      let data: &[u8] = if head.val_flag.is_infile() {
        let len = head.val_len.get() as usize;
        &self.read_data(loc, len).await?
      } else {
        return self.read_file_with_crc(loc.id(), head.val_crc32()).await;
      };
      let crc = crc32fast::hash(data);
      if crc != head.val_crc32() {
        return Err(Error::CrcMismatch(head.val_crc32(), crc));
      }
      Ok(data.to_vec())
    }
  }

  /// Read file with CRC check / 读取文件并校验 CRC
  async fn read_file_with_crc(&self, id: u64, expected_crc: u32) -> Result<Vec<u8>> {
    let data = self.read_file(id).await?;
    let crc = crc32fast::hash(&data);
    if crc != expected_crc {
      return Err(Error::CrcMismatch(expected_crc, crc));
    }
    Ok(data)
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

    let mut header_buf = vec![0u8; HEADER_SIZE];
    let res = file.read_exact_at(header_buf, 0).await;
    res.0?;
    header_buf = res.1;
    if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
      warn!("{LOG_SCAN_INVALID}: {path:?}");
      return Err(Error::InvalidHeader);
    }

    let mut pos = HEADER_SIZE as u64;
    let mut buf = vec![0u8; SCAN_BUF_SIZE];

    while pos < len {
      let read_len = ((len - pos) as usize).min(SCAN_BUF_SIZE);
      buf.truncate(read_len);

      let res = file.read_exact_at(std::mem::take(&mut buf), pos).await;
      res.0?;
      buf = res.1;

      let mut off = 0;
      while off + Head::SIZE <= buf.len() {
        let head = Head::read_from_bytes(&buf[off..off + Head::SIZE])
          .map_err(|_| Error::InvalidHead)?;

        if !f(pos + off as u64, &head) {
          return Ok(());
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

        let entry_len = Head::SIZE + data_len as usize;
        off += entry_len;

        // Entry spans beyond buffer / 条目跨越缓冲区边界
        if off > buf.len() {
          break;
        }
      }

      pos += off as u64;
    }

    Ok(())
  }
}
