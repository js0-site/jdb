//! WAL open/recover
//! WAL 打开/恢复

use std::{fs, sync::atomic::Ordering};

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::File;
use jdb_lock::WalLock;
use log::warn;
use memchr::memmem;

use super::{
  WalConf, WalInner,
  consts::{GC_SUBDIR, HEADER_SIZE, MIN_FILE_SIZE, SCAN_BUF_SIZE},
  header::{HeaderState, build_header, check_header},
};
use crate::{
  HEAD_TOTAL, Head, MAGIC, decode_id, error::Result, open_read_write, open_read_write_create,
};

impl<C: WalConf> WalInner<C> {
  pub async fn open(&mut self) -> Result<()> {
    fs::create_dir_all(&self.wal_dir)?;
    fs::create_dir_all(&self.bin_dir)?;

    let gc_dir = self.wal_dir.parent().unwrap().join(GC_SUBDIR);
    if gc_dir.exists() {
      let _ = fs::remove_dir_all(&gc_dir);
    }
    fs::create_dir_all(&gc_dir)?;

    if let Some((id, file, pos)) = self.find_newest().await {
      let path = self.wal_path(id);
      self.cur_lock.try_lock(&path)?;
      self.cur_id.store(id, Ordering::Release);
      *self.shared.file() = Some(file);
      self.cur_pos = pos;
      self.ider.init(id);
      return Ok(());
    }

    self.cur_id.store(self.ider.get(), Ordering::Release);
    self.create_wal_file().await
  }

  async fn create_wal_file(&mut self) -> Result<()> {
    let path = self.wal_path(self.cur_id());

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = open_read_write_create(&path).await?;

    file.write_all_at(build_header(), 0).await.0?;
    self.cur_pos = HEADER_SIZE as u64;
    *self.shared.file() = Some(file);
    self.cur_lock.try_lock(&path)?;
    Ok(())
  }

  #[allow(clippy::uninit_vec)]
  async fn find_newest(&self) -> Option<(u64, File, u64)> {
    let entries = fs::read_dir(&self.wal_dir).ok()?;

    let mut ids: Vec<u64> = entries
      .flatten()
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        decode_id(name)
      })
      .collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));

    let mut header_buf = Vec::with_capacity(HEADER_SIZE);

    for id in ids {
      let path = self.wal_path(id);
      let Ok(file) = open_read_write(&path).await else {
        continue;
      };

      let Ok(meta) = file.metadata().await else {
        continue;
      };

      let len = meta.len();
      if len < MIN_FILE_SIZE {
        warn!("WAL too small: {path:?}, len={len}");
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

  pub(crate) async fn rotate_inner(&mut self) -> Result<()> {
    // Sync old file before rotation
    // 轮转前同步旧文件
    if let Some(file) = self.shared.file() {
      file.sync_all().await?;
    }

    let new_id = self.ider.get();
    self.cur_id.store(new_id, Ordering::Release);
    let path = self.wal_path(new_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = open_read_write_create(&path).await?;

    file.write_all_at(build_header(), 0).await.0?;
    *self.shared.file() = Some(file);
    self.cur_pos = HEADER_SIZE as u64;
    self.cur_lock.try_lock(&path)?;
    Ok(())
  }
}

async fn recover(file: &File, checkpoint: u64, len: u64) -> u64 {
  if let Some(pos) = recover_forward(file, checkpoint, len).await {
    return pos;
  }

  if let Some(magic_pos) = search_backward(file, checkpoint, len).await {
    if let Some(end_pos) = recover_forward(file, magic_pos, len).await {
      return end_pos;
    }
    return magic_pos;
  }

  checkpoint
}

#[allow(clippy::uninit_vec)]
async fn recover_forward(file: &File, start: u64, len: u64) -> Option<u64> {
  let mut pos = start;
  let mut valid_end = None;
  let mut buf = Vec::with_capacity(SCAN_BUF_SIZE);

  while pos < len {
    let read_len = (len - pos).min(SCAN_BUF_SIZE as u64) as usize;
    if read_len < HEAD_TOTAL {
      break;
    }

    unsafe { buf.set_len(read_len) };
    let slice = buf.slice(0..read_len);
    let res = file.read_exact_at(slice, pos).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      break;
    }

    if buf[0] != MAGIC {
      break;
    }

    // Parse from Head (skip magic)
    // 从 Head 解析（跳过 magic）
    let head = match Head::parse(&buf[1..], 0, pos + 1) {
      Ok(h) => h,
      Err(_) => break,
    };

    // Disk size = magic(1) + record_size
    // 磁盘大小 = magic(1) + record_size
    let disk_size = 1 + head.record_size();
    if pos + disk_size as u64 > len {
      break;
    }

    pos += disk_size as u64;
    valid_end = Some(pos);
  }

  valid_end
}

#[allow(clippy::uninit_vec)]
async fn search_backward(file: &File, checkpoint: u64, len: u64) -> Option<u64> {
  let mut search_end = len;
  let mut buf = Vec::with_capacity(SCAN_BUF_SIZE);

  while search_end > checkpoint {
    let magic_pos = find_last_magic(file, checkpoint, search_end, &mut buf).await?;

    let read_len = (len - magic_pos).min(SCAN_BUF_SIZE as u64) as usize;
    if read_len >= HEAD_TOTAL {
      unsafe { buf.set_len(read_len) };
      let slice = buf.slice(0..read_len);
      let res = file.read_exact_at(slice, magic_pos).await;
      buf = res.1.into_inner();

      // Parse from Head (skip magic)
      // 从 Head 解析（跳过 magic）
      if res.0.is_ok() && buf[0] == MAGIC && Head::parse(&buf[1..], 0, magic_pos + 1).is_ok() {
        return Some(magic_pos);
      }
    }

    search_end = magic_pos;
  }

  None
}

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

    let tmp = std::mem::take(buf);
    let slice = tmp.slice(0..read_len);
    let res = file.read_exact_at(slice, read_start).await;
    *buf = res.1.into_inner();
    if res.0.is_err() {
      return None;
    }

    if let Some(idx) = memmem::rfind(buf, &[MAGIC]) {
      return Some(read_start + idx as u64);
    }

    if read_start == start {
      break;
    }
    pos = read_start + 1;
  }

  None
}
