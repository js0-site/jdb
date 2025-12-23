#![cfg_attr(docsrs, feature(doc_cfg))]

//! 预写日志 Write Ahead Log

use std::path::Path;

use jdb_alloc::AlignedBuf;
use jdb_fs::File;
use jdb_layout::crc32;

use crate::consts::{HEADER, PAGE_SIZE};
use crate::error::{E, R};

/// WAL 写入器 WAL writer
pub struct Writer {
  file: File,
  buf: AlignedBuf,
  pos: usize,  // buf 内写入位置
  offset: u64, // 文件写入偏移
  lsn: u64,    // 下一个 LSN
}

impl Writer {
  /// 创建新 WAL 文件 Create new WAL file
  pub async fn create(path: impl AsRef<Path>) -> R<Self> {
    let file = File::create(path).await?;
    Ok(Self {
      file,
      buf: AlignedBuf::zeroed(PAGE_SIZE),
      pos: 0,
      offset: 0,
      lsn: 1,
    })
  }

  /// 打开已有 WAL 文件 Open existing WAL file
  pub async fn open(path: impl AsRef<Path>) -> R<Self> {
    let file = File::open_rw(path).await?;
    let size = file.size().await?;
    Ok(Self {
      file,
      buf: AlignedBuf::zeroed(PAGE_SIZE),
      pos: 0,
      offset: size,
      lsn: 1,
    })
  }

  /// 设置 LSN (恢复后使用) Set LSN after recovery
  #[inline]
  pub fn set_lsn(&mut self, lsn: u64) {
    self.lsn = lsn;
  }

  /// 获取当前 LSN Get current LSN
  #[inline]
  pub fn lsn(&self) -> u64 {
    self.lsn
  }

  /// 追加记录 Append record, returns LSN
  pub async fn append(&mut self, data: &[u8]) -> R<u64> {
    let record_len = HEADER + data.len();

    // 超过单页大小 Exceeds single page
    if record_len > PAGE_SIZE {
      return Err(E::Full);
    }

    // 缓冲区空间不足 Buffer full
    if self.pos + record_len > PAGE_SIZE {
      self.flush().await?;
    }

    let lsn = self.lsn;
    let crc = crc32(data);

    // 写入 header: len(4) + crc(4) + lsn(8)
    self.buf[self.pos..self.pos + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    self.buf[self.pos + 4..self.pos + 8].copy_from_slice(&crc.to_le_bytes());
    self.buf[self.pos + 8..self.pos + 16].copy_from_slice(&lsn.to_le_bytes());
    self.buf[self.pos + 16..self.pos + record_len].copy_from_slice(data);

    self.pos += record_len;
    self.lsn += 1;

    Ok(lsn)
  }

  /// 刷新缓冲区 Flush buffer to disk
  pub async fn flush(&mut self) -> R<()> {
    if self.pos == 0 {
      return Ok(());
    }

    let buf = std::mem::replace(&mut self.buf, AlignedBuf::zeroed(PAGE_SIZE));
    let buf = self.file.write_at(self.offset, buf).await?;

    self.offset += PAGE_SIZE as u64;
    self.pos = 0;
    self.buf = buf;
    self.buf.fill(0);

    Ok(())
  }

  /// 同步到磁盘 Sync to disk
  pub async fn sync(&mut self) -> R<()> {
    self.flush().await?;
    self.file.sync().await?;
    Ok(())
  }
}

/// WAL 读取器 WAL reader
pub struct Reader {
  file: File,
  buf: AlignedBuf,
  pos: usize,     // buf 内读取位置
  buf_len: usize, // buf 有效长度
  offset: u64,    // 文件读取偏移
  file_size: u64,
}

impl Reader {
  /// 打开 WAL 文件 Open WAL file
  pub async fn open(path: impl AsRef<Path>) -> R<Self> {
    let file = File::open(path).await?;
    let file_size = file.size().await?;
    Ok(Self {
      file,
      buf: AlignedBuf::with_cap(0),
      pos: 0,
      buf_len: 0,
      offset: 0,
      file_size,
    })
  }

  /// 读取下一条记录 Read next record
  pub async fn next(&mut self) -> R<Option<(u64, Vec<u8>)>> {
    loop {
      // 需要读取更多数据
      if self.pos + HEADER > self.buf_len {
        if !self.fill_buf().await? {
          return Ok(None);
        }
      }

      let len = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;

      // len == 0 表示 padding
      if len == 0 {
        let remaining = PAGE_SIZE - (self.pos % PAGE_SIZE);
        self.pos += remaining;
        continue;
      }

      let crc = u32::from_le_bytes(self.buf[self.pos + 4..self.pos + 8].try_into().unwrap());
      let lsn = u64::from_le_bytes(self.buf[self.pos + 8..self.pos + 16].try_into().unwrap());

      let record_len = HEADER + len;

      // 确保有足够数据
      if self.pos + record_len > self.buf_len {
        if !self.fill_buf().await? || self.pos + record_len > self.buf_len {
          return Ok(None);
        }
      }

      let data = &self.buf[self.pos + HEADER..self.pos + record_len];

      // CRC 校验
      if crc32(data) != crc {
        return Ok(None);
      }

      self.pos += record_len;
      return Ok(Some((lsn, data.to_vec())));
    }
  }

  /// 填充缓冲区 Fill buffer
  async fn fill_buf(&mut self) -> R<bool> {
    if self.offset >= self.file_size {
      return Ok(false);
    }

    let to_read = ((self.file_size - self.offset) as usize).min(PAGE_SIZE);
    self.buf = self.file.read_at(self.offset, to_read).await?;
    self.buf_len = self.buf.len();
    self.offset += self.buf_len as u64;
    self.pos = 0;

    Ok(self.buf_len > 0)
  }

  /// 获取最后有效 LSN Get last valid LSN
  pub async fn last_lsn(&mut self) -> R<u64> {
    let mut last = 0u64;
    while let Some((lsn, _)) = self.next().await? {
      last = lsn;
    }
    Ok(last)
  }
}
