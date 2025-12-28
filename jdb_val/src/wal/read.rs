//! WAL read operations / WAL 读取操作

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAt, AsyncReadAtExt},
};
use compio_fs::{File, OpenOptions};
use jdb_lru::Cache;
use log::warn;
use zerocopy::FromBytes;

use super::{
  CachedData, Wal, WalConf, WalInner,
  consts::{HEADER_SIZE, ITER_BUF_SIZE, MAGIC_BYTES, MAGIC_SIZE, RECORD_HEADER_SIZE},
  header::{HeaderState, check_header},
  lz4,
};
use crate::{
  Head, Pos,
  error::{Error, Result},
};

impl<C: WalConf> WalInner<C> {
  /// Read head at location / 在位置读取头
  #[allow(clippy::uninit_vec)]
  pub async fn read_head(&mut self, loc: Pos) -> Result<Head> {
    if let Some(head) = self.head_cache.get(&loc) {
      return Ok(*head);
    }

    // Use helper to prepare buffer / 使用辅助函数准备缓冲区
    let buf = Wal::prepare_buf(&mut self.read_buf, Head::SIZE);
    let slice = buf.slice(0..Head::SIZE);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
    self.read_buf = slice.into_inner();
    res?;

    // SAFETY: read_exact_at success guarantees buf filled / 安全：成功时保证缓冲区已填满
    let head = unsafe { Head::read_from_bytes(self.read_buf.get_unchecked(..Head::SIZE)) }
      .map_err(|_| Error::InvalidHead)?;

    if !head.validate() {
      return Err(Error::InvalidHead);
    }
    self.head_cache.set(loc, head);
    Ok(head)
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
    // SAFETY: read_from_file success guarantees len bytes / 安全：成功时保证 len 字节
    let data: CachedData = unsafe { self.read_buf.get_unchecked(..len) }.into();
    self.data_cache.set(loc, data.clone());
    Ok(data)
  }

  /// Read from current or cached file / 从当前或缓存文件读取
  ///
  /// For current file, checks queue first
  /// 对于当前文件，先检查队列
  pub(crate) async fn read_from_file<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    mut buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    let res = if id == self.cur_id {
      // Check queue first / 先检查队列
      let len = buf.buf_capacity();
      if let Some(data) = self.shared.find_by_pos(pos, len) {
        // SAFETY: bounds checked in find_by_pos / 安全：find_by_pos 已检查边界
        unsafe {
          std::ptr::copy_nonoverlapping(data.as_ptr(), buf.as_buf_mut_ptr(), len);
          buf.set_buf_init(len);
        }
        return (Ok(()), buf);
      }
      // Flush queue before reading from file / 从文件读取前刷新队列
      if let Err(e) = self.flush().await {
        return (Err(e), buf);
      }
      // Read from file / 从文件读取
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
    if head.key_flag.is_inline() || head.key_flag.is_tombstone() {
      buf.extend_from_slice(head.key_data());
    } else if head.key_flag.is_infile() {
      // INFILE key: key compression not implemented / INFILE key: key 压缩未实现
      let loc = head.key_pos();
      self
        .read_infile_into(loc, head.key_len.get() as usize, buf)
        .await?;
    } else {
      // FILE mode / FILE 模式
      if head.key_flag.is_lz4() {
        // FILE_LZ4 key: read compressed file, decompress
        // FILE_LZ4 key: 读取压缩文件，解压缩
        let compressed = self.read_file(head.key_pos().id()).await?;
        lz4::decompress(&compressed, head.key_len.get() as usize, buf)?;
      } else {
        self.read_file_into(head.key_pos().id(), buf).await?;
      }
    }
    Ok(())
  }

  /// Get val by head / 根据头获取值
  pub async fn head_val(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.val_flag.is_inline() {
      Ok(head.val_data().to_vec())
    } else if head.val_flag.is_infile() {
      let loc = head.val_pos();
      let original_len = head.val_len.get() as usize;
      if head.val_flag.is_lz4() {
        // INFILE_LZ4: compressed_len stored in val_crc32
        // INFILE_LZ4: 压缩长度存储在 val_crc32
        let compressed_len = head.val_crc32() as usize;
        let compressed = self.read_data(loc, compressed_len).await?;
        let mut buf = Vec::new();
        lz4::decompress(&compressed, original_len, &mut buf)?;
        Ok(buf)
      } else {
        Ok(self.read_data(loc, original_len).await?.to_vec())
      }
    } else {
      // FILE mode / FILE 模式
      if head.val_flag.is_lz4() {
        // FILE_LZ4: read compressed file, decompress
        // FILE_LZ4: 读取压缩文件，解压缩
        let compressed = self.read_file(head.val_pos().id()).await?;
        let crc = crc32fast::hash(&compressed);
        if crc != head.val_crc32() {
          return Err(Error::CrcMismatch(head.val_crc32(), crc));
        }
        let original_len = head.val_len.get() as usize;
        let mut buf = Vec::new();
        lz4::decompress(&compressed, original_len, &mut buf)?;
        Ok(buf)
      } else {
        self
          .read_file_with_crc(head.val_pos().id(), head.val_crc32())
          .await
      }
    }
  }

  /// Read value into buffer / 读取值到缓冲区
  pub async fn read_val_into(&mut self, head: &Head, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    if head.val_flag.is_inline() || head.val_flag.is_tombstone() {
      buf.extend_from_slice(head.val_data());
    } else if head.val_flag.is_infile() {
      let loc = head.val_pos();
      let original_len = head.val_len.get() as usize;
      if head.val_flag.is_lz4() {
        // INFILE_LZ4: compressed_len stored in val_crc32
        // INFILE_LZ4: 压缩长度存储在 val_crc32
        let compressed_len = head.val_crc32() as usize;
        self.read_infile_into(loc, compressed_len, buf).await?;
        // Decompress in place / 原地解压缩
        let compressed = std::mem::take(buf);
        lz4::decompress(&compressed, original_len, buf)?;
      } else {
        self.read_infile_into(loc, original_len, buf).await?;
      }
    } else {
      // FILE mode / FILE 模式
      self.read_file_into(head.val_pos().id(), buf).await?;
      if head.val_flag.is_lz4() {
        // FILE_LZ4: decompress after reading
        // FILE_LZ4: 读取后解压缩
        let crc = crc32fast::hash(buf);
        if crc != head.val_crc32() {
          return Err(Error::CrcMismatch(head.val_crc32(), crc));
        }
        let original_len = head.val_len.get() as usize;
        let compressed = std::mem::take(buf);
        lz4::decompress(&compressed, original_len, buf)?;
      } else {
        // Verify CRC for file mode / 文件模式校验 CRC
        let crc = crc32fast::hash(buf);
        if crc != head.val_crc32() {
          return Err(Error::CrcMismatch(head.val_crc32(), crc));
        }
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
      buf: Vec::with_capacity(ITER_BUF_SIZE),
      buf_pos: HEADER_SIZE as u64,
      buf_cap: 0,
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
  /// Buffer for bulk reading / 批量读取缓冲区
  buf: Vec<u8>,
  /// Start position of the buffer in file / 缓冲区在文件中的起始位置
  buf_pos: u64,
  /// Valid bytes in buffer / 缓冲区内有效字节数
  buf_cap: usize,
}

impl LogIter {
  /// Read next entry / 读取下一个条目
  #[allow(clippy::uninit_vec)]
  pub async fn next(&mut self) -> Result<Option<(u64, Head)>> {
    // Check if we need to refill buffer / 检查是否需要填充缓冲区
    let mut off = (self.pos - self.buf_pos) as usize;

    if off + RECORD_HEADER_SIZE > self.buf_cap {
      if self.pos + RECORD_HEADER_SIZE as u64 > self.len {
        return Ok(None);
      }

      // Refill buffer from current pos / 从当前位置重新填充缓冲区
      let mut buf = std::mem::take(&mut self.buf);
      if buf.capacity() < ITER_BUF_SIZE {
        buf.reserve(ITER_BUF_SIZE - buf.capacity());
      }
      unsafe { buf.set_len(ITER_BUF_SIZE) };

      // Read at most ITER_BUF_SIZE, limited by file end
      // 最多读取 ITER_BUF_SIZE，受文件结束限制
      let read_len = (self.len - self.pos).min(ITER_BUF_SIZE as u64) as usize;
      let slice = buf.slice(0..read_len);
      let res = self.file.read_at(slice, self.pos).await;
      buf = res.1.into_inner();
      let n = res.0?;

      self.buf = buf;
      self.buf_pos = self.pos;
      self.buf_cap = n;
      off = 0;

      // Unexpected EOF / 意外的文件结束
      if n < RECORD_HEADER_SIZE {
        return Ok(None);
      }
    }

    // Verify magic / 验证 magic
    // SAFETY: off + RECORD_HEADER_SIZE <= buf_cap checked above
    // 安全：已在上文检查 off + RECORD_HEADER_SIZE <= buf_cap
    if unsafe { self.buf.get_unchecked(off..off + MAGIC_SIZE) } != MAGIC_BYTES {
      return Err(Error::InvalidMagic);
    }

    // Parse head / 解析 head
    let head = Head::read_from_bytes(unsafe {
      self
        .buf
        .get_unchecked(off + MAGIC_SIZE..off + RECORD_HEADER_SIZE)
    })
    .map_err(|_| Error::InvalidHead)?;

    if !head.validate() {
      return Err(Error::InvalidHead);
    }

    let k_len = if head.key_flag.is_infile() {
      head.key_len.get() as u64
    } else {
      0
    };
    // For INFILE_LZ4, compressed length is stored in val_crc32
    // 对于 INFILE_LZ4，压缩长度存储在 val_crc32
    let v_len = if head.val_flag.is_infile() {
      if head.val_flag.is_lz4() {
        head.val_crc32() as u64
      } else {
        head.val_len.get() as u64
      }
    } else {
      0
    };
    let entry_len = RECORD_HEADER_SIZE as u64 + k_len + v_len;

    if self.pos + entry_len <= self.len {
      let head_pos = self.pos + MAGIC_SIZE as u64;
      self.pos += entry_len;
      return Ok(Some((head_pos, head)));
    }

    Ok(None)
  }
}
