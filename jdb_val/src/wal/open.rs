//! WAL open/recover / WAL 打开/恢复

use std::fs;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::{File, OpenOptions};
use jdb_lru::Cache;
use log::warn;
use memchr::memmem::Finder;
use zerocopy::FromBytes;

use super::{
  CachedData, Wal, WalInner,
  consts::{HEADER_SIZE, MAGIC_BYTES, MAGIC_SIZE, RECORD_HEADER_SIZE, SCAN_BUF_SIZE},
  header::{HeaderState, build_header, check_header},
};
use crate::{Head, Pos, error::Result};

/// SIMD magic finder / SIMD 魔数查找器
static MAGIC_FINDER: std::sync::LazyLock<Finder<'static>> =
  std::sync::LazyLock::new(|| Finder::new(&MAGIC_BYTES));


impl<HC: Cache<Pos, Head>, DC: Cache<Pos, CachedData>> WalInner<HC, DC> {
  /// Open or create current WAL file / 打开或创建当前 WAL 文件
  pub async fn open(&mut self) -> Result<()> {
    fs::create_dir_all(&self.wal_dir)?;
    fs::create_dir_all(&self.bin_dir)?;

    if let Some((id, file, pos)) = self.find_newest().await {
      self.cur_id = id;
      *self.shared.file() = Some(file);
      self.cur_pos = pos;
      self.ider.init(id);
      return Ok(());
    }

    self.cur_id = self.ider.get();
    let path = self.wal_path(self.cur_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(build_header(), 0).await.0?;
    self.cur_pos = HEADER_SIZE as u64;
    *self.shared.file() = Some(file);
    Ok(())
  }

  /// Find newest valid WAL and recover / 找到最新的有效 WAL 并恢复
  #[allow(clippy::uninit_vec)]
  async fn find_newest(&self) -> Option<(u64, File, u64)> {
    let entries = fs::read_dir(&self.wal_dir).ok()?;

    let mut ids: Vec<u64> = entries
      .flatten()
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        Wal::decode_id(name)
      })
      .collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));

    let mut header_buf = Vec::with_capacity(HEADER_SIZE);

    for id in ids {
      let path = self.wal_path(id);
      let Ok(file) = OpenOptions::new().read(true).write(true).open(&path).await else {
        continue;
      };

      let Ok(meta) = file.metadata().await else {
        continue;
      };

      let len = meta.len();
      if len < HEADER_SIZE as u64 {
        warn!("WAL too small: {path:?}");
        continue;
      }

      unsafe { header_buf.set_len(HEADER_SIZE) };
      let slice = header_buf.slice(0..HEADER_SIZE);
      let res = file.read_exact_at(slice, 0).await;
      header_buf = res.1.into_inner();
      if res.0.is_err() {
        continue;
      }

      if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
        warn!("WAL header invalid: {path:?}");
        continue;
      }

      let valid_pos = recover(&file, HEADER_SIZE as u64, len).await;
      log::info!("WAL recovered: {path:?}, pos={valid_pos}");

      return Some((id, file, valid_pos));
    }

    None
  }

  /// Rotate to new WAL file / 轮转到新 WAL 文件
  pub async fn rotate(&mut self) -> Result<()> {
    self.flush().await?;

    self.cur_id = self.ider.get();
    let path = self.wal_path(self.cur_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(build_header(), 0).await.0?;
    *self.shared.file() = Some(file);
    self.cur_pos = HEADER_SIZE as u64;
    Ok(())
  }
}


/// Recover from checkpoint / 从检查点恢复
async fn recover(file: &File, checkpoint: u64, len: u64) -> u64 {
  if let Some(pos) = recover_forward(file, checkpoint, len).await {
    return pos;
  }

  // Forward failed, search backward / 向前失败，反向搜索
  if let Some(magic_pos) = search_backward(file, checkpoint, len).await {
    if let Some(end_pos) = recover_forward(file, magic_pos, len).await {
      return end_pos;
    }
    return magic_pos;
  }

  checkpoint
}

/// Forward recovery / 向前恢复
#[allow(clippy::uninit_vec)]
async fn recover_forward(file: &File, start: u64, len: u64) -> Option<u64> {
  let mut pos = start;
  let mut valid_end = None;
  let mut buf = Vec::with_capacity(RECORD_HEADER_SIZE);

  while pos + RECORD_HEADER_SIZE as u64 <= len {
    unsafe { buf.set_len(RECORD_HEADER_SIZE) };
    let slice = buf.slice(0..RECORD_HEADER_SIZE);
    let res = file.read_exact_at(slice, pos).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      break;
    }

    // Verify magic / 验证 magic
    if unsafe { buf.get_unchecked(..MAGIC_SIZE) } != MAGIC_BYTES {
      break;
    }

    // Verify head / 验证 head
    let head = match Head::read_from_bytes(unsafe { buf.get_unchecked(MAGIC_SIZE..) }) {
      Ok(h) if h.validate() => h,
      _ => break,
    };

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
    let entry_len = RECORD_HEADER_SIZE as u64 + k_len + v_len;

    if pos + entry_len > len {
      break;
    }

    pos += entry_len;
    valid_end = Some(pos);
  }

  valid_end
}

/// Search backward for valid magic+head / 反向搜索有效的 magic+head
#[allow(clippy::uninit_vec)]
async fn search_backward(file: &File, checkpoint: u64, len: u64) -> Option<u64> {
  let mut search_end = len;
  let mut buf = Vec::with_capacity(SCAN_BUF_SIZE);
  let mut head_buf = Vec::with_capacity(Head::SIZE);

  while search_end > checkpoint {
    // Find last magic / 找最后一个 magic
    let magic_pos = find_last_magic(file, checkpoint, search_end, &mut buf).await?;

    // Verify head / 验证 head
    if magic_pos + RECORD_HEADER_SIZE as u64 <= len {
      unsafe { head_buf.set_len(Head::SIZE) };
      let slice = head_buf.slice(0..Head::SIZE);
      let res = file.read_exact_at(slice, magic_pos + MAGIC_SIZE as u64).await;
      head_buf = res.1.into_inner();

      if res.0.is_ok() {
        if let Ok(head) = Head::read_from_bytes(&head_buf) {
          if head.validate() {
            return Some(magic_pos);
          }
        }
      }
    }

    search_end = magic_pos;
  }

  None
}

/// Find last magic in range / 在范围内找最后一个 magic
#[allow(clippy::uninit_vec)]
async fn find_last_magic(file: &File, start: u64, end: u64, buf: &mut Vec<u8>) -> Option<u64> {
  if end <= start {
    return None;
  }

  let mut pos = end;

  while pos > start {
    let read_start = pos.saturating_sub(SCAN_BUF_SIZE as u64).max(start);
    let read_len = (pos - read_start) as usize;

    if buf.capacity() < read_len {
      buf.reserve(read_len - buf.capacity());
    }
    unsafe { buf.set_len(read_len) };

    let slice = buf.slice(0..read_len);
    let res = file.read_exact_at(slice, read_start).await;
    *buf = res.1.into_inner();
    if res.0.is_err() {
      return None;
    }

    if let Some(idx) = MAGIC_FINDER.rfind(buf) {
      return Some(read_start + idx as u64);
    }

    // Overlap 3 bytes / 重叠 3 字节
    pos = read_start.saturating_add(3).min(read_start);
    if pos == read_start {
      break;
    }
  }

  None
}
