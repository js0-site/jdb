//! WAL read operations
//! WAL 读取操作

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAt, AsyncReadAtExt},
};
use compio_fs::{File, OpenOptions};
use jdb_lru::Cache;
use log::warn;

use super::{
  CachedData, Wal, WalConf, WalInner,
  consts::{HEADER_SIZE, ITER_BUF_SIZE},
  header::{HeaderState, check_header},
  lz4,
};
use crate::{
  FIXED_SIZE, Head, MAGIC, Pos,
  error::{Error, Result},
};

impl<C: WalConf> WalInner<C> {
  /// Read head at location
  /// 在位置读取头
  #[allow(clippy::uninit_vec)]
  pub async fn read_head(&mut self, loc: Pos) -> Result<Head> {
    if let Some(head) = self.head_cache.get(&loc) {
      return Ok(head.clone());
    }

    // Read enough for variable-length head (use read_at, not read_exact_at)
    // 读取足够的变长头（使用 read_at，不是 read_exact_at）
    let buf = Wal::prepare_buf(&mut self.read_buf, ITER_BUF_SIZE);
    let slice = buf.slice(0..ITER_BUF_SIZE);
    let (res, slice) = self.read_at_partial(loc.id(), slice, loc.pos()).await;
    self.read_buf = slice.into_inner();
    res?;

    let head = Head::parse(&self.read_buf)?;
    self.head_cache.set(loc, head.clone());
    Ok(head)
  }

  /// Read head_data for a head at location
  /// 读取位置处头的 head_data
  #[allow(clippy::uninit_vec)]
  pub async fn read_head_data(&mut self, loc: Pos, head: &Head) -> Result<CachedData> {
    let data_pos = Pos::new(loc.id(), loc.pos() + head.data_off as u64);
    let len = head.head_len as usize;

    if let Some(data) = self.data_cache.get(&data_pos) {
      return Ok(data.clone());
    }

    let buf = Wal::prepare_buf(&mut self.read_buf, len);
    let slice = buf.slice(0..len);
    let (res, slice) = self.read_from_file(loc.id(), slice, data_pos.pos()).await;
    self.read_buf = slice.into_inner();
    res?;
    let data: CachedData = unsafe { self.read_buf.get_unchecked(..len) }.into();
    self.data_cache.set(data_pos, data.clone());
    Ok(data)
  }

  /// Read at position (may read less than requested)
  /// 在位置读取（可能读取少于请求的量）
  async fn read_at_partial<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    mut buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    let len = buf.buf_capacity();
    if id == self.cur_id() {
      // Try queue first
      // 先尝试队列
      if let Some(data) = self.shared.find_by_pos(pos, len) {
        unsafe {
          std::ptr::copy_nonoverlapping(data.as_ptr(), buf.as_buf_mut_ptr(), len);
          buf.set_buf_init(len);
        }
        return (Ok(()), buf);
      }
      if let Err(e) = self.flush().await {
        return (Err(e), buf);
      }
      if let Some(file) = self.shared.file() {
        let res = file.read_at(buf, pos).await;
        return (res.0.map_err(Into::into).map(drop), res.1);
      } else {
        return (Err(Error::NotOpen), buf);
      }
    }
    match self.get_file(id).await {
      Ok(file) => {
        let res = file.read_at(buf, pos).await;
        (res.0.map_err(Into::into).map(drop), res.1)
      }
      Err(e) => (Err(e), buf),
    }
  }

  pub(super) async fn get_file(&mut self, id: u64) -> Result<&File> {
    self.get_cached_file(id, true).await
  }

  /// Read data from WAL file (Infile mode only, cached)
  /// 从 WAL 文件读取数据（仅 Infile 模式，有缓存）
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
    let data: CachedData = unsafe { self.read_buf.get_unchecked(..len) }.into();
    self.data_cache.set(loc, data.clone());
    Ok(data)
  }

  /// Read from current or cached file
  /// 从当前或缓存文件读取
  pub(crate) async fn read_from_file<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    mut buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    let res = if id == self.cur_id() {
      let len = buf.buf_capacity();
      if let Some(data) = self.shared.find_by_pos(pos, len) {
        unsafe {
          std::ptr::copy_nonoverlapping(data.as_ptr(), buf.as_buf_mut_ptr(), len);
          buf.set_buf_init(len);
        }
        return (Ok(()), buf);
      }
      if let Err(e) = self.flush().await {
        return (Err(e), buf);
      }
      if let Some(file) = self.shared.file() {
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

  /// Read data from separate file (File mode, not cached)
  /// 从独立文件读取数据（File 模式，无缓存）
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
    buf.extend_from_slice(unsafe { self.read_buf.get_unchecked(..len) });
    Ok(())
  }

  pub async fn read_file(&mut self, id: u64) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_file_into(id, &mut buf).await?;
    Ok(buf)
  }

  /// Get key by head and head_data
  /// 根据头和头数据获取键
  pub async fn get_key(&mut self, head: &Head, head_data: &[u8]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_key_into(head, head_data, &mut buf).await?;
    Ok(buf)
  }

  /// Read key into buffer
  /// 读取键到缓冲区
  pub async fn read_key_into(
    &mut self,
    head: &Head,
    head_data: &[u8],
    buf: &mut Vec<u8>,
  ) -> Result<()> {
    buf.clear();
    let store = head.key_store();
    if store.is_infile() {
      // INFILE: data in head_data
      // INFILE: 数据在 head_data 中
      let key_data = head.key_data(head_data);
      if store.is_lz4() {
        lz4::decompress(key_data, head.key_len as usize, buf)?;
      } else {
        buf.extend_from_slice(key_data);
      }
    } else {
      // FILE mode
      // FILE 模式
      let fpos = head.key_file_pos(head_data);
      self.read_file_into(fpos.file_id, buf).await?;
      if !fpos.verify(buf) {
        return Err(Error::HashMismatch);
      }
      if store.is_lz4() {
        let compressed = std::mem::take(buf);
        lz4::decompress(&compressed, head.key_len as usize, buf)?;
      }
    }
    Ok(())
  }

  /// Get val by head and head_data
  /// 根据头和头数据获取值
  pub async fn get_val(&mut self, head: &Head, head_data: &[u8]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_val_into(head, head_data, &mut buf).await?;
    Ok(buf)
  }

  /// Get val by pos (convenience method)
  /// 根据位置获取值（便捷方法）
  pub async fn val(&mut self, pos: Pos) -> Result<Vec<u8>> {
    let head = self.read_head(pos).await?;
    let head_data = self.read_head_data(pos, &head).await?;
    self.get_val(&head, &head_data).await
  }

  /// Get key by pos (convenience method)
  /// 根据位置获取键（便捷方法）
  pub async fn key(&mut self, pos: Pos) -> Result<Vec<u8>> {
    let head = self.read_head(pos).await?;
    let head_data = self.read_head_data(pos, &head).await?;
    self.get_key(&head, &head_data).await
  }

  /// Get key and val by pos (convenience method)
  /// 根据位置获取键值对（便捷方法）
  pub async fn kv(&mut self, pos: Pos) -> Result<(Vec<u8>, Vec<u8>)> {
    let head = self.read_head(pos).await?;
    let head_data = self.read_head_data(pos, &head).await?;
    let key = self.get_key(&head, &head_data).await?;
    let val = self.get_val(&head, &head_data).await?;
    Ok((key, val))
  }

  /// Read value into buffer
  /// 读取值到缓冲区
  pub async fn read_val_into(
    &mut self,
    head: &Head,
    head_data: &[u8],
    buf: &mut Vec<u8>,
  ) -> Result<()> {
    buf.clear();
    if head.is_tombstone() {
      return Ok(());
    }

    let store = head.val_store();
    let val_len = head.val_len.unwrap_or(0) as usize;

    if store.is_infile() {
      // INFILE: data in head_data
      // INFILE: 数据在 head_data 中
      let val_data = head.val_data(head_data);
      if store.is_lz4() {
        lz4::decompress(val_data, val_len, buf)?;
      } else {
        buf.extend_from_slice(val_data);
      }
    } else {
      // FILE mode
      // FILE 模式
      let fpos = head.val_file_pos(head_data);
      self.read_file_into(fpos.file_id, buf).await?;
      if !fpos.verify(buf) {
        return Err(Error::HashMismatch);
      }
      if store.is_lz4() {
        let compressed = std::mem::take(buf);
        lz4::decompress(&compressed, val_len, buf)?;
      }
    }
    Ok(())
  }

  /// Get an iterator over entries in a WAL file
  /// 获取 WAL 文件条目迭代器
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
      buf: Vec::with_capacity(ITER_BUF_SIZE),
      buf_pos: HEADER_SIZE as u64,
      buf_cap: 0,
    })
  }

  /// Scan all entries in a WAL file
  /// 扫描 WAL 文件中的所有条目
  pub async fn scan<F>(&self, id: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, &Head, &[u8]) -> bool,
  {
    let mut iter = self.iter_entries(id).await?;
    while let Some((pos, head, data)) = iter.next().await? {
      if !f(pos, &head, data) {
        break;
      }
    }
    Ok(())
  }
}

/// WAL Log Iterator
/// WAL 日志迭代器
pub struct LogIter {
  file: File,
  pos: u64,
  len: u64,
  buf: Vec<u8>,
  buf_pos: u64,
  buf_cap: usize,
}

impl LogIter {
  /// Read next entry, returns (pos, head, head_data)
  /// 读取下一个条目，返回 (位置, 头, 头数据)
  #[allow(clippy::uninit_vec)]
  pub async fn next(&mut self) -> Result<Option<(u64, Head, &[u8])>> {
    let mut off = (self.pos - self.buf_pos) as usize;

    if off + FIXED_SIZE > self.buf_cap {
      if self.pos + FIXED_SIZE as u64 > self.len {
        return Ok(None);
      }

      let mut buf = std::mem::take(&mut self.buf);
      if buf.capacity() < ITER_BUF_SIZE {
        buf.reserve(ITER_BUF_SIZE - buf.capacity());
      }
      unsafe { buf.set_len(ITER_BUF_SIZE) };

      let read_len = (self.len - self.pos).min(ITER_BUF_SIZE as u64) as usize;
      let slice = buf.slice(0..read_len);
      let res = self.file.read_at(slice, self.pos).await;
      buf = res.1.into_inner();
      let n = res.0?;

      self.buf = buf;
      self.buf_pos = self.pos;
      self.buf_cap = n;
      off = 0;

      if n < FIXED_SIZE {
        return Ok(None);
      }
    }

    // Check magic
    // 检查魔数
    if unsafe { *self.buf.get_unchecked(off) } != MAGIC {
      return Err(Error::InvalidMagic);
    }

    // Parse head
    // 解析头
    let head = Head::parse(unsafe { self.buf.get_unchecked(off..) })?;

    // Check if complete record in buffer
    // 检查完整记录是否在缓冲区中
    if off + head.size > self.buf_cap {
      // Need to read more
      // 需要读取更多
      if self.pos + head.size as u64 > self.len {
        return Ok(None);
      }

      let mut buf = std::mem::take(&mut self.buf);
      let need = head.size.max(ITER_BUF_SIZE);
      if buf.capacity() < need {
        buf.reserve(need - buf.capacity());
      }
      unsafe { buf.set_len(need) };

      let read_len = (self.len - self.pos).min(need as u64) as usize;
      let slice = buf.slice(0..read_len);
      let res = self.file.read_at(slice, self.pos).await;
      buf = res.1.into_inner();
      let n = res.0?;

      self.buf = buf;
      self.buf_pos = self.pos;
      self.buf_cap = n;
      off = 0;

      if n < head.size {
        return Ok(None);
      }
    }

    let head_pos = self.pos;
    self.pos += head.size as u64;

    // Return head_data slice
    // 返回 head_data 切片
    let data_start = off + head.data_off;
    let data_end = data_start + head.head_len as usize;
    let head_data = unsafe { self.buf.get_unchecked(data_start..data_end) };

    Ok(Some((head_pos, head, head_data)))
  }
}
