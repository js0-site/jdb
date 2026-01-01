//! WAL read operations
//! WAL 读取操作

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAt, AsyncReadAtExt},
};
use compio_fs::File;
use jdb_base::{HEAD_CRC, HEAD_TOTAL, Head, MAGIC, Pos};
use log::warn;
use size_lru::SizeLru;

use super::{
  Val, Wal, WalConf, WalInner,
  consts::{HEADER_SIZE, ITER_BUF_SIZE, SMALL_BUF_SIZE},
  header::{HeaderState, check_header},
  record::Record,
};
use crate::{
  error::{Error, Result},
};

use jdb_base::open_read;

impl<C: WalConf> WalInner<C> {
  /// Read head at location
  /// 在位置读取头
  #[allow(clippy::uninit_vec)]
  pub async fn read_head(&mut self, loc: Record) -> Result<Head> {
    let buf = Wal::prepare_buf(&mut self.read_buf, HEAD_CRC);
    let slice = buf.slice(0..HEAD_CRC);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.offset()).await;
    self.read_buf = slice.into_inner();
    res?;

    Ok(Head::parse(&self.read_buf, loc.id(), loc.offset())?)
  }

  /// Read full record at location
  /// 在位置读取完整记录
  #[allow(clippy::uninit_vec)]
  #[inline(always)]
  pub async fn read_record(&mut self, loc: Record) -> Result<(Head, Val)> {
    let buf = Wal::prepare_buf(&mut self.read_buf, SMALL_BUF_SIZE);
    let slice = buf.slice(0..SMALL_BUF_SIZE);
    let (res, slice) = self.read_at_partial(loc.id(), slice, loc.offset()).await;
    self.read_buf = slice.into_inner();
    res?;

    let head = Head::parse(&self.read_buf, loc.id(), loc.offset())?;
    let record_size = head.record_size();

    // Fast path: record fits in buffer
    // 快速路径：记录在缓冲区内
    if record_size <= self.read_buf.len() {
      let data: Val = self.read_buf[..record_size].into();
      return Ok((head, data));
    }

    // Slow path: need larger read
    // 慢速路径：需要更大的读取
    let buf = Wal::prepare_buf(&mut self.read_buf, record_size);
    let slice = buf.slice(0..record_size);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.offset()).await;
    self.read_buf = slice.into_inner();
    res?;

    let data: Val = self.read_buf[..record_size].into();
    Ok((head, data))
  }

  async fn read_at_partial<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    self.read_impl(id, buf, pos, false).await
  }

  /// Read data from WAL file (Infile mode only, cached)
  /// 从 WAL 文件读取数据（仅 Infile 模式，有缓存）
  #[allow(clippy::uninit_vec)]
  pub async fn read_data(&mut self, loc: Pos, len: usize) -> Result<Val> {
    if let Some(data) = self.val_cache.get(&loc) {
      return Ok(data.clone());
    }

    let buf = Wal::prepare_buf(&mut self.read_buf, len);
    let slice = buf.slice(0..len);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.offset()).await;
    self.read_buf = slice.into_inner();
    res?;
    let data: Val = self.read_buf[..len].into();
    self.val_cache.set(loc, data.clone(), len as u32);
    Ok(data)
  }

  pub(crate) async fn read_from_file<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    self.read_impl(id, buf, pos, true).await
  }

  #[inline(always)]
  async fn read_impl<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    mut buf: B,
    pos: u64,
    exact: bool,
  ) -> (Result<()>, B) {
    let len = buf.buf_capacity();
    let cur = self.cur_id();

    if id == cur {
      // Try to find in write buffer first
      // 先尝试从写缓冲区查找
      if let Some((ptr, actual)) = self.shared.find_by_pos(pos, len)
        && (actual >= len || !exact)
      {
        // Full data in buffer, copy directly
        // 完整数据在缓冲区，直接复制
        let copy_len = if exact { len } else { actual };
        unsafe {
          std::ptr::copy_nonoverlapping(ptr, buf.as_buf_mut_ptr(), copy_len);
          buf.set_buf_init(copy_len);
        }
        return (Ok(()), buf);
      }

      // Read from file
      // 从文件读取
      let Some(file) = self.shared.file() else {
        return (Err(Error::NotOpen), buf);
      };
      return if exact {
        let res = file.read_exact_at(buf, pos).await;
        (res.0.map_err(Into::into).map(drop), res.1)
      } else {
        let res = file.read_at(buf, pos).await;
        (res.0.map_err(Into::into).map(drop), res.1)
      };
    }

    match self.block_cache.read_into(id, buf, pos).await {
      (Ok(()), buf) => (Ok(()), buf),
      (Err(e), buf) => (Err(Error::Io(e)), buf),
    }
  }

  /// Read data from separate file (File mode, not cached)
  /// 从独立文件读取数据（File 模式，无缓存）
  #[allow(clippy::uninit_vec)]
  pub async fn read_file_into(&mut self, id: u64, buf: &mut Vec<u8>) -> Result<()> {
    let file = self.get_bin_file(id).await?;
    let len = file.metadata().await?.len() as usize;

    // Direct read to caller's buffer (zero-copy)
    // 直接读到调用者缓冲区（零拷贝）
    buf.clear();
    buf.reserve(len);
    unsafe { buf.set_len(len) };

    let slice = std::mem::take(buf).slice(0..len);
    let res = file.read_exact_at(slice, 0).await;
    *buf = res.1.into_inner();
    res.0?;
    Ok(())
  }

  pub async fn read_file(&mut self, id: u64) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_file_into(id, &mut buf).await?;
    Ok(buf)
  }

  /// Get val by pos (direct read, no head parsing)
  /// 根据位置获取值（直接读取，无需解析 head）
  #[inline(always)]
  pub async fn val(&mut self, pos: Pos) -> Result<Val> {
    // Fast path: cache hit (sync)
    // 快速路径：缓存命中（同步）
    if let Some(data) = self.val_cache.get(&pos) {
      return Ok(data.clone());
    }

    // Slow path: cache miss (async IO)
    // 慢速路径：缓存未命中（异步 IO）
    self.val_slow(pos).await
  }

  /// Try get val from cache only (sync, no IO)
  /// 仅从缓存获取值（同步，无 IO）
  #[inline(always)]
  pub fn val_cached(&mut self, pos: &Pos) -> Option<Val> {
    self.val_cache.get(pos).cloned()
  }

  /// Slow path for val() - cache miss
  /// val() 的慢速路径 - 缓存未命中
  #[cold]
  async fn val_slow(&mut self, pos: Pos) -> Result<Val> {
    let len = pos.len() as usize;
    if len == 0 {
      return Ok(Val::from([]));
    }

    if pos.is_infile() {
      // INFILE: read val directly
      // INFILE：直接读取 val
      let buf = Wal::prepare_buf(&mut self.read_buf, len);
      let slice = buf.slice(0..len);
      let (res, slice) = self.read_from_file(pos.id(), slice, pos.offset()).await;
      self.read_buf = slice.into_inner();
      res?;
      let data: Val = self.read_buf[..len].into();
      self.val_cache.set(pos, data.clone(), len as u32);
      Ok(data)
    } else {
      // FILE: read from separate file
      // FILE：从独立文件读取
      let mut buf = Vec::new();
      self.read_file_into(pos.file_id(), &mut buf).await?;
      Ok(buf.into())
    }
  }

  /// Get key by record pos
  /// 根据记录位置获取键
  pub async fn key(&mut self, pos: Record) -> Result<Vec<u8>> {
    let (head, record) = self.read_record(pos).await?;
    Ok(head.key_data(&record).to_vec())
  }

  /// Get key and val by record pos
  /// 根据记录位置获取键值对
  pub async fn kv(&mut self, pos: Record) -> Result<(Vec<u8>, Vec<u8>)> {
    let (head, record) = self.read_record(pos).await?;
    let key = head.key_data(&record).to_vec();

    let mut val = Vec::new();
    self.read_val_into(&head, &record, &mut val).await?;
    Ok((key, val))
  }

  /// Read value into buffer (raw data, no decompression)
  /// 读取值到缓冲区（原始数据，不解压）
  pub async fn read_val_into(
    &mut self,
    head: &Head,
    record: &[u8],
    buf: &mut Vec<u8>,
  ) -> Result<()> {
    buf.clear();
    if head.is_tombstone() {
      return Ok(());
    }

    if head.val_is_infile() {
      let val = head.val_data(record);
      buf.extend_from_slice(val);
    } else {
      self.read_file_into(head.val_file_id, buf).await?;
    }
    Ok(())
  }

  /// Get an iterator over entries in a WAL file
  /// 获取 WAL 文件条目迭代器
  #[allow(clippy::uninit_vec)]
  pub async fn iter_entries(&self, id: u64) -> Result<LogIter> {
    let path = self.wal_path(id);
    let file = open_read(&path).await?;
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
    while let Some((pos, head, record)) = iter.next().await? {
      if !f(pos, &head, record) {
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
  /// Read next entry, returns (head_pos, head, record)
  /// head_pos is Head position (excludes magic)
  /// 读取下一个条目，返回 (Head位置, 头, 记录)
  /// head_pos 是 Head 位置（不含 magic）
  #[allow(clippy::uninit_vec)]
  pub async fn next(&mut self) -> Result<Option<(u64, Head, &[u8])>> {
    let mut off = (self.pos - self.buf_pos) as usize;

    // Need to read magic(1) + HEAD_CRC
    // 需要读取 magic(1) + HEAD_CRC
    if off + HEAD_TOTAL > self.buf_cap {
      if self.pos + HEAD_TOTAL as u64 > self.len {
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

      if n < HEAD_TOTAL {
        return Ok(None);
      }
    }

    if unsafe { *self.buf.get_unchecked(off) } != MAGIC {
      return Err(Error::InvalidMagic);
    }

    // Parse from Head (skip magic)
    // 从 Head 解析（跳过 magic）
    let head_pos = self.pos + 1;
    let head = Head::parse(unsafe { self.buf.get_unchecked(off + 1..) }, 0, head_pos)?;
    // Total record size on disk = magic(1) + record_size
    // 磁盘上的总记录大小 = magic(1) + record_size
    let disk_size = 1 + head.record_size();

    if off + disk_size > self.buf_cap {
      if self.pos + disk_size as u64 > self.len {
        return Ok(None);
      }

      let mut buf = std::mem::take(&mut self.buf);
      let need = disk_size.max(ITER_BUF_SIZE);
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

      if n < disk_size {
        return Ok(None);
      }
    }

    self.pos += disk_size as u64;

    // Record buffer starts from Head (skip magic)
    // 记录缓冲区从 Head 开始（跳过 magic）
    let record = unsafe { self.buf.get_unchecked(off + 1..off + disk_size) };
    Ok(Some((head_pos, head, record)))
  }
}
