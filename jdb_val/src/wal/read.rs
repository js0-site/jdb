//! WAL read operations / WAL 读取操作

use compio::{
  buf::{IntoInner, IoBuf},
  io::AsyncReadAtExt,
};
use compio_fs::{File, OpenOptions};
use log::warn;
use zerocopy::FromBytes;

use super::{
  CachedData, Wal,
  consts::{END_SIZE, HEADER_SIZE},
  header::{HeaderState, check_header},
};
use crate::{
  Head, Pos,
  error::{Error, Result},
};

impl Wal {
  /// Read head at location / 在位置读取头
  #[allow(clippy::uninit_vec)]
  pub async fn read_head(&mut self, loc: Pos) -> Result<Head> {
    if let Some(head) = self.head_cache.get(&loc) {
      return Ok(*head);
    }

    // Use helper to prepare buffer / 使用辅助函数准备缓冲区
    let buf = Wal::prepare_buf(&mut self.scratch, Head::SIZE);
    let slice = buf.slice(0..Head::SIZE);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
    self.scratch = slice.into_inner();
    res?;

    // SAFETY: read_exact_at success guarantees buf filled / 安全：成功时保证缓冲区已填满
    let head = unsafe { Head::read_from_bytes(self.scratch.get_unchecked(..Head::SIZE)) }
      .map_err(|_| Error::InvalidHead)?;

    if !head.validate() {
      return Err(Error::InvalidHead);
    }
    self.head_cache.insert(loc, head);
    Ok(head)
  }

  pub(super) async fn get_file(&mut self, id: u64) -> Result<&File> {
    self.get_cached_file(id, true).await
  }

  /// Read data from WAL file / 从 WAL 文件读取数据
  #[allow(clippy::uninit_vec)]
  pub async fn read_data(&mut self, loc: Pos, len: usize) -> Result<CachedData> {
    if let Some(data) = self.data_cache.get(&loc) {
      return Ok(data.clone());
    }

    let buf = Wal::prepare_buf(&mut self.read_buf, len);
    let slice = buf.slice(0..len);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
    self.read_buf = slice.into_inner();
    res?;
    // SAFETY: read_from_file success guarantees len bytes / 安全：成功时保证 len 字节
    let data: CachedData = unsafe { self.read_buf.get_unchecked(..len) }.into();
    // Cache if enabled / 启用缓存时插入
    if self.data_cache.capacity() > 0 {
      self.data_cache.insert(loc, data.clone());
    }
    Ok(data)
  }

  /// Read from current or cached file / 从当前或缓存文件读取
  pub(crate) async fn read_from_file<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    let res = if id == self.cur_id {
      let file_ref = self.cur_file.borrow();
      if let Some(file) = file_ref.as_ref() {
        file.read_exact_at(buf, pos).await
      } else {
        return (Err(Error::NotOpen), buf);
      }
    } else {
      match self.get_file(id).await {
        Ok(file) => file.read_exact_at(buf, pos).await,
        Err(e) => return (Err(e), buf),
      }
    };
    (res.0.map_err(Into::into).map(drop), res.1)
  }

  /// Read infile data into buffer / 读取 infile 数据到缓冲区
  #[allow(clippy::uninit_vec)]
  async fn read_infile_into(&mut self, loc: Pos, len: usize, buf: &mut Vec<u8>) -> Result<()> {
    let tmp = Wal::prepare_buf(&mut self.read_buf, len);
    let slice = tmp.slice(0..len);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
    self.read_buf = slice.into_inner();
    res?;
    // SAFETY: read_from_file success guarantees len bytes / 安全：成功时保证 len 字节
    buf.extend_from_slice(unsafe { self.read_buf.get_unchecked(..len) });
    Ok(())
  }

  /// Read data from separate file into buffer / 从独立文件读取数据到缓冲区
  #[allow(clippy::uninit_vec)]
  pub async fn read_file_into(&mut self, id: u64, buf: &mut Vec<u8>) -> Result<()> {
    let len = {
      let file = self.get_cached_file(id, false).await?;
      file.metadata().await?.len() as usize
    };

    let tmp = Wal::prepare_buf(&mut self.read_buf, len);
    let slice = tmp.slice(0..len);
    let file = self.get_cached_file(id, false).await?;
    let res = file.read_exact_at(slice, 0).await;
    self.read_buf = res.1.into_inner();
    res.0?;
    // SAFETY: read_exact_at success guarantees len bytes / 安全：成功时保证 len 字节
    buf.extend_from_slice(unsafe { self.read_buf.get_unchecked(..len) });
    Ok(())
  }

  pub async fn read_file(&mut self, id: u64) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_file_into(id, &mut buf).await?;
    Ok(buf)
  }

  /// Get key by head / 根据头获取键
  pub async fn head_key(&mut self, head: &Head) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_key_into(head, &mut buf).await?;
    Ok(buf)
  }

  /// Read key into buffer / 读取键到缓冲区
  pub async fn read_key_into(&mut self, head: &Head, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    // Tombstone with inline key uses key_flag = TOMBSTONE / 内联键的删除标记使用 key_flag = TOMBSTONE
    if head.key_flag.is_inline() || head.key_flag.is_tombstone() {
      buf.extend_from_slice(head.key_data());
    } else if head.key_flag.is_infile() {
      self
        .read_infile_into(head.key_pos(), head.key_len.get() as usize, buf)
        .await?;
    } else {
      self.read_file_into(head.key_pos().id(), buf).await?;
    }
    Ok(())
  }

  /// Get val by head / 根据头获取值
  pub async fn head_val(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.val_flag.is_inline() {
      Ok(head.val_data().to_vec())
    } else if head.val_flag.is_infile() {
      let loc = head.val_pos();
      let len = head.val_len.get() as usize;
      Ok(self.read_data(loc, len).await?.to_vec())
    } else {
      self
        .read_file_with_crc(head.val_pos().id(), head.val_crc32())
        .await
    }
  }

  /// Read value into buffer / 读取值到缓冲区
  pub async fn read_val_into(&mut self, head: &Head, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    if head.val_flag.is_inline() {
      buf.extend_from_slice(head.val_data());
    } else if head.val_flag.is_infile() {
      self
        .read_infile_into(head.val_pos(), head.val_len.get() as usize, buf)
        .await?;
    } else {
      self.read_file_into(head.val_pos().id(), buf).await?;
      let crc = crc32fast::hash(buf);
      if crc != head.val_crc32() {
        return Err(Error::CrcMismatch(head.val_crc32(), crc));
      }
    }
    Ok(())
  }

  /// Read file with CRC check / 读取文件并校验 CRC
  async fn read_file_with_crc(&mut self, id: u64, expected: u32) -> Result<Vec<u8>> {
    let data = self.read_file(id).await?;
    let crc = crc32fast::hash(&data);
    if crc != expected {
      return Err(Error::CrcMismatch(expected, crc));
    }
    Ok(data)
  }

  /// Get an iterator over entries in a WAL file / 获取 WAL 文件条目迭代器
  #[allow(clippy::uninit_vec)]
  pub async fn iter_entries(&self, id: u64) -> Result<LogIter> {
    let path = self.wal_path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len();

    if len < HEADER_SIZE as u64 {
      warn!("WAL too small: {path:?}");
      return Err(Error::InvalidHeader);
    }

    let mut buf = Vec::with_capacity(HEADER_SIZE);
    unsafe { buf.set_len(HEADER_SIZE) };
    let slice = buf.slice(0..HEADER_SIZE);
    let res = file.read_exact_at(slice, 0).await;
    res.0?;
    let mut buf = res.1.into_inner();
    if matches!(check_header(&mut buf), HeaderState::Invalid) {
      warn!("WAL header invalid: {path:?}");
      return Err(Error::InvalidHeader);
    }

    Ok(LogIter {
      file,
      pos: HEADER_SIZE as u64,
      len,
      buf: Vec::with_capacity(Head::SIZE),
    })
  }

  /// Scan all entries in a WAL file / 扫描 WAL 文件中的所有条目
  pub async fn scan<F>(&self, id: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, &Head) -> bool,
  {
    let mut iter = self.iter_entries(id).await?;
    while let Some((pos, head)) = iter.next().await? {
      if !f(pos, &head) {
        break;
      }
    }
    Ok(())
  }
}

/// WAL Log Iterator / WAL 日志迭代器
pub struct LogIter {
  file: File,
  pos: u64,
  len: u64,
  buf: Vec<u8>,
}

impl LogIter {
  /// Read next entry / 读取下一个条目
  #[allow(clippy::uninit_vec)]
  pub async fn next(&mut self) -> Result<Option<(u64, Head)>> {
    if self.pos >= self.len {
      return Ok(None);
    }

    let buf = Wal::prepare_buf(&mut self.buf, Head::SIZE);
    let slice = buf.slice(0..Head::SIZE);
    let res = self.file.read_exact_at(slice, self.pos).await;
    self.buf = res.1.into_inner();
    res.0?;

    // SAFETY: read_exact_at success guarantees buf filled / 安全：成功时保证缓冲区已填满
    let head = Head::read_from_bytes(unsafe { self.buf.get_unchecked(..Head::SIZE) })
      .map_err(|_| Error::InvalidHead)?;

    if !head.validate() {
      return Err(Error::InvalidHead);
    }

    let k_len = if head.key_flag.is_infile() {
      head.key_len.get() as u64
    } else {
      0
    };
    let v_len = if head.val_flag.is_infile() {
      head.val_len.get() as u64
    } else {
      0
    };
    let entry_len = (Head::SIZE + END_SIZE) as u64 + k_len + v_len;

    if self.pos + entry_len <= self.len {
      let entry_pos = self.pos;
      self.pos += entry_len;
      return Ok(Some((entry_pos, head)));
    }

    Ok(None)
  }
}
