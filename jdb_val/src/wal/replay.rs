//! WAL replay stream for recovery
//! WAL 回放流用于恢复
//!
//! Returns async stream of (key, Pos) for rebuilding index
//! 返回 (key, Pos) 异步流用于重建索引

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAt, AsyncReadAtExt},
};
use compio_fs::File;
use jdb_base::{HEAD_CRC, HEAD_TOTAL, Head, Load, MAGIC, Pos, id_path, open_read};
use log::warn;

use super::{
  WalConf, WalEntry, WalInner,
  consts::{HEADER_SIZE, MIN_FILE_SIZE, SCAN_BUF_SIZE},
  header::{HeaderState, check_header},
};
use crate::{Result, WalPtr, error::Error};

/// Replay item: (key, Pos)
/// 回放项：(key, Pos)
pub type ReplayItem = (Vec<u8>, Pos);

/// WAL replay iterator (independent read stream)
/// WAL 回放迭代器（独立读取流）
pub struct ReplayIter {
  wal_dir: std::path::PathBuf,
  /// WAL file IDs to replay
  /// 需要回放的 WAL 文件 ID
  file_ids: Vec<u64>,
  /// Current file index
  /// 当前文件索引
  file_idx: usize,
  /// Current file (owned by iterator)
  /// 当前文件（迭代器拥有）
  file: Option<File>,
  /// Current WAL ID
  /// 当前 WAL ID
  cur_id: u64,
  /// Current position in file
  /// 文件中的当前位置
  pos: u64,
  /// File length
  /// 文件长度
  len: u64,
  /// Read buffer
  /// 读取缓冲区
  buf: Vec<u8>,
  /// Buffer start position in file
  /// 缓冲区在文件中的起始位置
  buf_pos: u64,
  /// Valid data length in buffer
  /// 缓冲区中的有效数据长度
  buf_cap: usize,
  /// Start offset for first file
  /// 第一个文件的起始偏移
  start_offset: u64,
  /// End pointer (WAL open position)
  /// 终点指针（WAL 打开位置）
  end_ptr: WalPtr,
  /// Done flag
  /// 完成标志
  done: bool,
}

impl ReplayIter {
  /// Create new replay iterator
  /// 创建新的回放迭代器
  ///
  /// - file_ids: files to replay [save.id, rotate1, ...]
  /// - start_offset: offset in first file
  /// - end_ptr: stop position (WAL open position)
  pub fn new(
    wal_dir: std::path::PathBuf,
    file_ids: Vec<u64>,
    start_offset: u64,
    end_ptr: WalPtr,
  ) -> Self {
    Self {
      wal_dir,
      file_ids,
      file_idx: 0,
      file: None,
      cur_id: 0,
      pos: HEADER_SIZE as u64,
      len: 0,
      buf: Vec::with_capacity(SCAN_BUF_SIZE),
      buf_pos: 0,
      buf_cap: 0,
      start_offset,
      end_ptr,
      done: false,
    }
  }

  /// Get next replay item
  /// 获取下一个回放项
  pub async fn next(&mut self) -> Result<Option<ReplayItem>> {
    if self.done {
      return Ok(None);
    }

    loop {
      // Check if reached end point
      // 检查是否到达终点
      if self.cur_id == self.end_ptr.id && self.pos >= self.end_ptr.offset {
        self.done = true;
        return Ok(None);
      }
      if self.cur_id > self.end_ptr.id {
        self.done = true;
        return Ok(None);
      }

      // Open next file if needed
      // 如果需要，打开下一个文件
      if self.file.is_none() && !self.open_next_file().await? {
        self.done = true;
        return Ok(None);
      }

      // Try read entry
      // 尝试读取条目
      match self.read_entry().await {
        Ok(Some(item)) => return Ok(Some(item)),
        Ok(None) => {
          // EOF, try next file
          // 文件结束，尝试下一个文件
          self.file = None;
        }
        Err(Error::InvalidMagic | Error::CrcMismatch { .. }) => {
          // Corruption, search for next magic
          // 损坏，搜索下一个 magic
          if let Some(magic_pos) = self.search_forward_magic().await {
            self.pos = magic_pos;
            self.buf_cap = 0;
          } else {
            // No more valid records, try next file
            // 没有更多有效记录，尝试下一个文件
            self.file = None;
          }
        }
        Err(e) => return Err(e),
      }
    }
  }

  /// Open next WAL file
  /// 打开下一个 WAL 文件
  async fn open_next_file(&mut self) -> Result<bool> {
    while self.file_idx < self.file_ids.len() {
      let id = self.file_ids[self.file_idx];
      let is_first = self.file_idx == 0;
      self.file_idx += 1;

      // First file starts at start_offset, others at HEADER_SIZE
      // 第一个文件从 start_offset 开始，其他从 HEADER_SIZE 开始
      self.pos = if is_first {
        self.start_offset
      } else {
        HEADER_SIZE as u64
      };

      let path = id_path(&self.wal_dir, id);
      let file = match open_read(&path).await {
        Ok(f) => f,
        Err(_) => continue,
      };

      let meta = match file.metadata().await {
        Ok(m) => m,
        Err(_) => continue,
      };

      let len = meta.len();
      if len < MIN_FILE_SIZE {
        warn!("WAL too small: {path:?}, len={len}");
        continue;
      }

      // Validate header
      // 验证头
      let mut header_buf = vec![0u8; HEADER_SIZE];
      let slice = header_buf.slice(0..HEADER_SIZE);
      let res = file.read_exact_at(slice, 0).await;
      if res.0.is_err() {
        continue;
      }
      header_buf = res.1.into_inner();

      if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
        warn!("WAL header invalid: {path:?}");
        continue;
      }

      self.file = Some(file);
      self.cur_id = id;
      self.len = len;
      self.buf_cap = 0;
      return Ok(true);
    }

    Ok(false)
  }

  /// Read single entry from current file
  /// 从当前文件读取单个条目
  #[allow(clippy::uninit_vec)]
  async fn read_entry(&mut self) -> Result<Option<ReplayItem>> {
    let file = match &self.file {
      Some(f) => f,
      None => return Ok(None),
    };

    // Ensure buffer has enough data for header
    // 确保缓冲区有足够的头数据
    let mut off = (self.pos - self.buf_pos) as usize;
    if off + HEAD_TOTAL > self.buf_cap {
      if self.pos + HEAD_TOTAL as u64 > self.len {
        return Ok(None);
      }

      self.buf.clear();
      if self.buf.capacity() < SCAN_BUF_SIZE {
        self.buf.reserve(SCAN_BUF_SIZE - self.buf.capacity());
      }
      unsafe { self.buf.set_len(SCAN_BUF_SIZE) };

      let read_len = (self.len - self.pos).min(SCAN_BUF_SIZE as u64) as usize;
      let tmp = std::mem::take(&mut self.buf);
      let slice = tmp.slice(0..read_len);
      let res = file.read_at(slice, self.pos).await;
      self.buf = res.1.into_inner();
      let n = res.0?;

      self.buf_pos = self.pos;
      self.buf_cap = n;
      off = 0;

      if n < HEAD_TOTAL {
        return Ok(None);
      }
    }

    // Check magic
    // 检查 magic
    if unsafe { *self.buf.get_unchecked(off) } != MAGIC {
      return Err(Error::InvalidMagic);
    }

    // Parse head (skip magic)
    // 解析头（跳过 magic）
    let head_pos = self.pos + 1;
    let head = Head::parse(unsafe { self.buf.get_unchecked(off + 1..) }, 0, head_pos)?;
    let disk_size = 1 + head.record_size();

    // Ensure buffer has full record
    // 确保缓冲区有完整记录
    if off + disk_size > self.buf_cap {
      if self.pos + disk_size as u64 > self.len {
        return Ok(None);
      }

      let need = disk_size.max(SCAN_BUF_SIZE);
      self.buf.clear();
      if self.buf.capacity() < need {
        self.buf.reserve(need - self.buf.capacity());
      }
      unsafe { self.buf.set_len(need) };

      let read_len = (self.len - self.pos).min(need as u64) as usize;
      let tmp = std::mem::take(&mut self.buf);
      let slice = tmp.slice(0..read_len);
      let res = file.read_at(slice, self.pos).await;
      self.buf = res.1.into_inner();
      let n = res.0?;

      self.buf_pos = self.pos;
      self.buf_cap = n;
      off = 0;

      if n < disk_size {
        return Ok(None);
      }
    }

    // Extract key and build Pos
    // 提取 key 并构建 Pos
    let record = unsafe { self.buf.get_unchecked(off + 1..off + disk_size) };
    let key = head.key_data(record).to_vec();

    let flag = head.flag();
    let pos = if flag.is_tombstone() {
      Pos::tombstone(self.cur_id, head_pos + HEAD_CRC as u64)
    } else if flag.is_infile() {
      // INFILE: val offset = head_pos + HEAD_CRC
      // INFILE：val 偏移 = head_pos + HEAD_CRC
      Pos::infile_with_flag(flag, self.cur_id, head_pos + HEAD_CRC as u64, head.val_len)
    } else {
      // FILE: file_id stored in head
      // FILE：file_id 存储在 head 中
      Pos::file_with_flag(flag, self.cur_id, head.val_file_id, head.val_len)
    };

    self.pos += disk_size as u64;
    Ok(Some((key, pos)))
  }

  /// Search forward for next magic byte
  /// 向前搜索下一个 magic 字节
  #[allow(clippy::uninit_vec)]
  async fn search_forward_magic(&mut self) -> Option<u64> {
    let file = self.file.as_ref()?;
    let mut search_pos = self.pos + 1;

    while search_pos < self.len {
      let read_len = (self.len - search_pos).min(SCAN_BUF_SIZE as u64) as usize;
      if read_len == 0 {
        break;
      }

      self.buf.clear();
      if self.buf.capacity() < read_len {
        self.buf.reserve(read_len - self.buf.capacity());
      }
      unsafe { self.buf.set_len(read_len) };

      let tmp = std::mem::take(&mut self.buf);
      let slice = tmp.slice(0..read_len);
      let res = file.read_at(slice, search_pos).await;
      self.buf = res.1.into_inner();
      if res.0.is_err() {
        break;
      }

      // Find magic in buffer
      // 在缓冲区中查找 magic
      if let Some(idx) = WalEntry::find_magic(&self.buf) {
        let magic_pos = search_pos + idx as u64;

        // Validate head at magic position
        // 验证 magic 位置的头
        if magic_pos + HEAD_TOTAL as u64 <= self.len {
          let head_start = idx + 1;
          if head_start + HEAD_CRC <= self.buf.len()
            && Head::parse(&self.buf[head_start..], 0, magic_pos + 1).is_ok()
          {
            return Some(magic_pos);
          }
        }

        // Invalid head, continue searching
        // 无效头，继续搜索
        search_pos = magic_pos + 1;
      } else {
        search_pos += read_len as u64;
      }
    }

    None
  }
}

impl<C: WalConf> WalInner<C> {
  /// Internal: create replay iterator from loaded checkpoint info
  /// 内部：从加载的检查点信息创建回放迭代器
  pub(crate) fn create_replay_iter(&self, replay_info: Option<(WalPtr, Vec<u64>)>) -> ReplayIter {
    let end_ptr = WalPtr::new(self.cur_id(), self.cur_pos);

    let (file_ids, start_offset) = match replay_info {
      Some((ptr, ids)) => (ids, ptr.offset),
      None => (Vec::new(), HEADER_SIZE as u64),
    };

    ReplayIter::new(self.wal_dir.clone(), file_ids, start_offset, end_ptr)
  }

  /// Open WAL and return async iterator for replay (standalone use)
  /// 打开 WAL 并返回异步迭代器用于回放（独立使用）
  pub fn replay(&self, start_ptr: Option<WalPtr>) -> ReplayIter {
    let end_ptr = WalPtr::new(self.cur_id(), self.cur_pos);

    let (file_ids, start_offset) = match start_ptr {
      Some(ptr) => {
        // Collect file ids >= ptr.id
        // 收集 id >= ptr.id 的文件
        let mut ids: Vec<u64> = self.iter().filter(|&id| id >= ptr.id).collect();
        ids.sort_unstable();
        (ids, ptr.offset)
      }
      None => {
        let mut ids: Vec<u64> = self.iter().collect();
        ids.sort_unstable();
        (ids, HEADER_SIZE as u64)
      }
    };

    ReplayIter::new(self.wal_dir.clone(), file_ids, start_offset, end_ptr)
  }
}
