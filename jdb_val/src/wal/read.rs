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

    // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
    let mut buf = std::mem::take(&mut self.scratch);
    if buf.capacity() < Head::SIZE {
      buf.reserve(Head::SIZE - buf.len());
    }
    unsafe { buf.set_len(Head::SIZE) };

    let slice = buf.slice(0..Head::SIZE);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
    // Restore scratch buffer regardless of result / 无论结果如何都归还 scratch buffer
    self.scratch = slice.into_inner();
    res?;

    // SAFETY: read_exact_at guarantees buffer is filled if successful
    // 安全：read_exact_at 成功时保证缓冲区已填满
    let head = unsafe { Head::read_from_bytes(self.scratch.get_unchecked(..Head::SIZE)) }
      .map_err(|_| Error::InvalidHead)?;
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

    // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
    let mut buf = Vec::with_capacity(len);
    unsafe { buf.set_len(len) };

    let slice = buf.slice(0..len);
    let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
    res?;
    let data: CachedData = slice.into_inner().into();
    self.data_cache.insert(loc, data.clone());
    Ok(data)
  }

  /// Helper to read from either current or cached file / 从当前或缓存文件读取的辅助函数
  ///
  /// Returns buffer back even on error to avoid reallocation
  /// 即使出错也归还 buffer 以避免重新分配
  pub(crate) async fn read_from_file<B: compio::buf::IoBufMut>(
    &mut self,
    id: u64,
    buf: B,
    pos: u64,
  ) -> (Result<()>, B) {
    let res = if id == self.cur_id {
      if let Some(file) = &self.cur_file {
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
    (res.0.map_err(Error::from).map(|_| ()), res.1)
  }

  /// Read data from separate file into buffer / 从独立文件读取数据到缓冲区
  #[allow(clippy::uninit_vec)]
  pub async fn read_file_into(&mut self, id: u64, buf: &mut Vec<u8>) -> Result<()> {
    // Get file length first / 先获取文件长度
    let len = {
      let file = self.get_cached_file(id, false).await?;
      file.metadata().await?.len() as usize
    };

    // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
    let mut tmp = std::mem::take(&mut self.read_buf);
    if tmp.capacity() < len {
      tmp.reserve(len - tmp.len());
    }
    unsafe { tmp.set_len(len) };

    let slice = tmp.slice(0..len);
    // Re-get file reference / 重新获取文件引用
    let file = self.get_cached_file(id, false).await?;
    let res = file.read_exact_at(slice, 0).await;
    self.read_buf = res.1.into_inner();
    res.0?;
    buf.clear();
    buf.extend_from_slice(&self.read_buf);
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

  /// Read key directly into buffer (zero copy for cache) / 直接读取键到缓冲区
  #[allow(clippy::uninit_vec)]
  pub async fn read_key_into(&mut self, head: &Head, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    if head.key_flag.is_inline() {
      buf.extend_from_slice(head.key_data());
      Ok(())
    } else {
      let loc = head.key_pos();
      if head.key_flag.is_infile() {
        let len = head.key_len.get() as usize;
        // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
        let mut tmp = std::mem::take(&mut self.read_buf);
        if tmp.capacity() < len {
          tmp.reserve(len - tmp.len());
        }
        unsafe { tmp.set_len(len) };
        let slice = tmp.slice(0..len);
        let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
        self.read_buf = slice.into_inner();
        res?;
        buf.extend_from_slice(&self.read_buf);
        Ok(())
      } else {
        self.read_file_into(loc.id(), buf).await
      }
    }
  }

  /// Get val by head / 根据头获取值
  pub async fn head_val(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.val_flag.is_inline() {
      Ok(head.val_data().to_vec())
    } else {
      let loc = head.val_pos();
      if head.val_flag.is_infile() {
        // Infile: no CRC check, relies on WAL integrity / Infile: 无需 CRC 校验，依赖 WAL 完整性
        let len = head.val_len.get() as usize;
        Ok(self.read_data(loc, len).await?.to_vec())
      } else {
        self.read_file_with_crc(loc.id(), head.val_crc32()).await
      }
    }
  }

  /// Read value directly into buffer (zero copy for cache) / 直接读取值到缓冲区
  #[allow(clippy::uninit_vec)]
  pub async fn read_val_into(&mut self, head: &Head, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    if head.val_flag.is_inline() {
      buf.extend_from_slice(head.val_data());
      Ok(())
    } else {
      let loc = head.val_pos();
      if head.val_flag.is_infile() {
        // Infile: no CRC check, relies on WAL integrity / Infile: 无需 CRC 校验，依赖 WAL 完整性
        // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
        let len = head.val_len.get() as usize;
        let mut tmp = std::mem::take(&mut self.read_buf);
        if tmp.capacity() < len {
          tmp.reserve(len - tmp.len());
        }
        unsafe { tmp.set_len(len) };
        let slice = tmp.slice(0..len);
        let (res, slice) = self.read_from_file(loc.id(), slice, loc.pos()).await;
        self.read_buf = slice.into_inner();
        res?;
        buf.extend_from_slice(&self.read_buf);
        Ok(())
      } else {
        self.read_file_into(loc.id(), buf).await?;
        let crc = crc32fast::hash(buf);
        if crc != head.val_crc32() {
          return Err(Error::CrcMismatch(head.val_crc32(), crc));
        }
        Ok(())
      }
    }
  }

  /// Read file with CRC check / 读取文件并校验 CRC
  async fn read_file_with_crc(&mut self, id: u64, expected_crc: u32) -> Result<Vec<u8>> {
    let data = self.read_file(id).await?;
    let crc = crc32fast::hash(&data);
    if crc != expected_crc {
      return Err(Error::CrcMismatch(expected_crc, crc));
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

    // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
    let mut header_buf = Vec::with_capacity(HEADER_SIZE);
    unsafe { header_buf.set_len(HEADER_SIZE) };
    let slice = header_buf.slice(0..HEADER_SIZE);
    let res = file.read_exact_at(slice, 0).await;
    res.0?;
    let mut header_buf = res.1.into_inner();
    if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
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

    // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
    let mut buf = std::mem::take(&mut self.buf);
    if buf.capacity() < Head::SIZE {
      buf.reserve(Head::SIZE - buf.len());
    }
    unsafe { buf.set_len(Head::SIZE) };

    let slice = buf.slice(0..Head::SIZE);
    let res = self.file.read_exact_at(slice, self.pos).await;
    // Put buffer back immediately / 立即归还 buffer
    self.buf = res.1.into_inner();
    res.0?;

    // SAFETY: read_exact_at success guarantees buf.len() == Head::SIZE
    // 安全：read_exact_at 成功保证 buf.len() == Head::SIZE
    let head = Head::read_from_bytes(unsafe { self.buf.get_unchecked(..Head::SIZE) })
      .map_err(|_| Error::InvalidHead)?;

    // Calc infile data length / 计算 infile 数据长度
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

    // Check if entry valid in file range / 检查条目在文件范围内有效
    if self.pos + entry_len <= self.len {
      let entry_pos = self.pos;
      self.pos += entry_len;
      return Ok(Some((entry_pos, head)));
    }

    Ok(None)
  }
}
