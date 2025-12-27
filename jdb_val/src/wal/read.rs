//! WAL read operations / WAL 读取操作

use compio::io::AsyncReadAtExt;
use compio_fs::{File, OpenOptions};
use hashlink::lru_cache::Entry;
use log::warn;
use zerocopy::FromBytes;

use super::{
  CachedData, Wal,
  consts::{END_SIZE, HEADER_SIZE, SCAN_BUF_SIZE},
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

    // Optimization: Reuse scratch buffer / 优化：复用 scratch 缓冲区
    let mut buf = std::mem::take(&mut self.scratch);
    buf.clear();
    buf.reserve(Head::SIZE);
    unsafe { buf.set_len(Head::SIZE) };

    let res = self.read_from_file(loc.id(), buf, loc.pos()).await;

    // Restore scratch or drop if error / 恢复 scratch 或在错误时丢弃
    let buf = match res {
      Ok(b) => b,
      Err(e) => return Err(e),
    };

    // Note: compio may return buffer with original capacity as length
    // 注意：compio 可能返回原始容量作为长度的缓冲区
    let head =
      Head::read_from_bytes(&buf[..Head::SIZE.min(buf.len())]).map_err(|_| Error::InvalidHead)?;
    self.scratch = buf;
    self.head_cache.insert(loc, head);
    Ok(head)
  }

  pub(super) async fn get_file(&mut self, id: u64) -> Result<&File> {
    match self.file_cache.entry(id) {
      Entry::Occupied(e) => Ok(e.into_mut()),
      Entry::Vacant(e) => {
        let path = self
          .wal_dir
          .join(fast32::base32::CROCKFORD_LOWER.encode_u64(id));
        let file = OpenOptions::new().read(true).open(&path).await?;
        Ok(e.insert(file))
      }
    }
  }

  /// Read data from WAL file / 从 WAL 文件读取数据
  #[allow(clippy::uninit_vec)]
  pub async fn read_data(&mut self, loc: Pos, len: usize) -> Result<CachedData> {
    if let Some(data) = self.data_cache.get(&loc) {
      return Ok(data.clone());
    }

    // Optimization: Avoid zero-initialization for data / 优化：避免数据的零初始化
    let mut buf = Vec::with_capacity(len);
    // SAFETY: read_exact_at will overwrite the buffer. If IO fails, buffer is dropped.
    // 安全：read_exact_at 会覆盖缓冲区。如果 IO 失败，缓冲区会被丢弃。
    unsafe { buf.set_len(len) };

    let buf = self.read_from_file(loc.id(), buf, loc.pos()).await?;
    let data: CachedData = buf.into();
    self.data_cache.insert(loc, data.clone());
    Ok(data)
  }

  /// Helper to read from either current or cached file / 从当前或缓存文件读取的辅助函数
  async fn read_from_file(&mut self, id: u64, buf: Vec<u8>, pos: u64) -> Result<Vec<u8>> {
    let res = if id == self.cur_id {
      let file = self.cur_file.as_ref().ok_or(Error::NotOpen)?;
      file.read_exact_at(buf, pos).await
    } else {
      let file = self.get_file(id).await?;
      file.read_exact_at(buf, pos).await
    };
    Ok(res.0.map(|_| res.1)?)
  }

  /// Read data from separate file into buffer / 从独立文件读取数据到缓冲区
  #[allow(clippy::uninit_vec)]
  pub async fn read_file_into(&self, id: u64, buf: &mut Vec<u8>) -> Result<()> {
    let path = self.bin_path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len() as usize;

    // Reset buffer to ensure clean overwrite / 重置缓冲区以确保覆盖写入
    buf.clear();
    buf.reserve(len);
    // SAFETY: read_exact_at will overwrite the buffer / read_exact_at 会覆盖缓冲区
    unsafe { buf.set_len(len) };

    let res = file.read_exact_at(std::mem::take(buf), 0).await;
    res.0?;
    *buf = res.1;
    Ok(())
  }

  pub async fn read_file(&self, id: u64) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    self.read_file_into(id, &mut buf).await?;
    Ok(buf)
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
  pub async fn read_val_into(&mut self, head: &Head, buf: &mut Vec<u8>) -> Result<()> {
    // Ensure consistent behavior (overwrite) by clearing first / 先清空以确保一致的行为（覆盖）
    buf.clear();
    if head.val_flag.is_inline() {
      buf.extend_from_slice(head.val_data());
      Ok(())
    } else {
      let loc = head.val_pos();
      if head.val_flag.is_infile() {
        // Infile: no CRC check, relies on WAL integrity / Infile: 无需 CRC 校验，依赖 WAL 完整性
        let len = head.val_len.get() as usize;
        let data = self.read_data(loc, len).await?;
        buf.extend_from_slice(&data);
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
  async fn read_file_with_crc(&self, id: u64, expected_crc: u32) -> Result<Vec<u8>> {
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

    // Optimization: Avoid zero-initialization / 优化：避免零初始化
    let mut header_buf = Vec::with_capacity(HEADER_SIZE);
    unsafe { header_buf.set_len(HEADER_SIZE) };
    let res = file.read_exact_at(header_buf, 0).await;
    res.0?;
    let mut header_buf = res.1;
    if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
      warn!("WAL header invalid: {path:?}");
      return Err(Error::InvalidHeader);
    }

    Ok(LogIter {
      file,
      pos: HEADER_SIZE as u64,
      len,
      buf: Vec::with_capacity(SCAN_BUF_SIZE),
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

    let read_len = (self.len - self.pos).min(SCAN_BUF_SIZE as u64) as usize;

    // SAFETY: read_exact_at guarantees filling the buffer or returning error
    // 安全：read_exact_at 保证填满缓冲区或返回错误
    self.buf.clear();
    self.buf.reserve(read_len);
    unsafe { self.buf.set_len(read_len) };

    let res = self
      .file
      .read_exact_at(std::mem::take(&mut self.buf), self.pos)
      .await;
    res.0?;
    self.buf = res.1;

    if Head::SIZE <= self.buf.len() {
      // SAFETY: bounds checked above / 边界已检查
      let head = Head::read_from_bytes(unsafe { self.buf.get_unchecked(..Head::SIZE) })
        .map_err(|_| Error::InvalidHead)?;

      let data_len = if head.key_flag.is_infile() {
        head.key_len.get() as u64
      } else {
        0
      } + if head.val_flag.is_infile() {
        head.val_len.get() as u64
      } else {
        0
      };

      let entry_len = Head::SIZE + data_len as usize + END_SIZE;

      // Check if entry valid in file range (fix: don't check buf len for large entries)
      // 检查条目在文件范围内有效（修复：大条目不检查 buf 长度）
      if self.pos + entry_len as u64 <= self.len {
        let entry_pos = self.pos;
        self.pos += entry_len as u64;
        return Ok(Some((entry_pos, head)));
      }
    }

    Ok(None)
  }
}
