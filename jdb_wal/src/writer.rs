//! WAL writer WAL 写入器

use jdb_alloc::AlignedBuf;
use jdb_comm::{JdbResult, Lsn, PAGE_SIZE};
use jdb_fs::File;
use jdb_layout::{crc32, decode, encode, WalEntry};
use std::path::Path;

/// Entry header size (len:u32 + crc:u32) 条目头大小
const ENTRY_HEADER: usize = 8;

/// WAL writer WAL 写入器
pub struct WalWriter {
  file: File,
  buf: AlignedBuf,
  lsn: Lsn,
  offset: u64,
}

impl WalWriter {
  /// Create new WAL file 创建新 WAL 文件
  pub async fn create(path: impl AsRef<Path>) -> JdbResult<Self> {
    let file = File::create(path).await?;
    Ok(Self {
      file,
      buf: AlignedBuf::with_cap(PAGE_SIZE * 4),
      lsn: Lsn::new(0),
      offset: 0,
    })
  }

  /// Open existing WAL 打开已有 WAL
  pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self> {
    let file = File::open_rw(path).await?;
    let offset = file.size().await?;
    Ok(Self {
      file,
      buf: AlignedBuf::with_cap(PAGE_SIZE * 4),
      lsn: Lsn::new(0),
      offset,
    })
  }

  /// Current LSN 当前 LSN
  #[inline]
  pub fn lsn(&self) -> Lsn {
    self.lsn
  }

  /// Append entry 追加条目
  pub fn append(&mut self, entry: &WalEntry) -> JdbResult<Lsn> {
    let data = encode(entry);
    let len = data.len() as u32;
    let crc = crc32(&data);

    // Write header: len + crc 写入头：长度 + 校验和
    self.buf.extend(&len.to_le_bytes());
    self.buf.extend(&crc.to_le_bytes());
    self.buf.extend(&data);

    self.lsn = self.lsn.next();
    Ok(self.lsn)
  }

  /// Flush buffer to disk 刷新缓冲区到磁盘
  pub async fn flush(&mut self) -> JdbResult<()> {
    if self.buf.is_empty() {
      return Ok(());
    }

    // Pad to page boundary 填充到页边界
    let pad = (PAGE_SIZE - (self.buf.len() % PAGE_SIZE)) % PAGE_SIZE;
    if pad > 0 && pad < PAGE_SIZE {
      let zeros = vec![0u8; pad];
      self.buf.extend(&zeros);
    }

    let buf = std::mem::replace(&mut self.buf, AlignedBuf::with_cap(PAGE_SIZE * 4));
    let len = buf.len() as u64;

    let _ = self.file.write_at(self.offset, buf).await?;
    self.file.sync().await?;

    self.offset += len;
    Ok(())
  }

  /// Append and flush 追加并刷新
  pub async fn append_sync(&mut self, entry: &WalEntry) -> JdbResult<Lsn> {
    let lsn = self.append(entry)?;
    self.flush().await?;
    Ok(lsn)
  }
}

/// WAL reader for recovery 恢复用 WAL 读取器
pub struct WalReader {
  data: Vec<u8>,
  pos: usize,
}

impl WalReader {
  /// Open WAL for reading 打开 WAL 读取
  pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self> {
    let file = File::open(path).await?;
    let size = file.size().await?;

    if size == 0 {
      return Ok(Self {
        data: Vec::new(),
        pos: 0,
      });
    }

    // Read all pages 读取所有页
    let pages = ((size + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64) as usize;
    let mut data = Vec::with_capacity(pages * PAGE_SIZE);

    for i in 0..pages {
      let buf = file.read_page(i as u32).await?;
      data.extend_from_slice(&buf);
    }

    // Truncate to actual size 截断到实际大小
    data.truncate(size as usize);

    Ok(Self { data, pos: 0 })
  }

  /// Read next entry 读取下一条目
  pub fn next(&mut self) -> JdbResult<Option<WalEntry>> {
    if self.pos + ENTRY_HEADER > self.data.len() {
      return Ok(None);
    }

    let len = u32::from_le_bytes([
      self.data[self.pos],
      self.data[self.pos + 1],
      self.data[self.pos + 2],
      self.data[self.pos + 3],
    ]) as usize;

    let expected_crc = u32::from_le_bytes([
      self.data[self.pos + 4],
      self.data[self.pos + 5],
      self.data[self.pos + 6],
      self.data[self.pos + 7],
    ]);

    // Zero length means padding 零长度表示填充
    if len == 0 {
      return Ok(None);
    }

    let data_start = self.pos + ENTRY_HEADER;
    let data_end = data_start + len;

    if data_end > self.data.len() {
      return Ok(None);
    }

    let data = &self.data[data_start..data_end];
    let actual_crc = crc32(data);

    if actual_crc != expected_crc {
      return Err(jdb_comm::JdbError::Checksum {
        expected: expected_crc,
        actual: actual_crc,
      });
    }

    self.pos = data_end;

    let entry = decode(data).map_err(|e| jdb_comm::JdbError::Serialize(e.to_string().into()))?;

    Ok(Some(entry))
  }
}
