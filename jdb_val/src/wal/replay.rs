//! WAL replay for recovery
//! WAL 回放用于恢复

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAt, AsyncReadAtExt},
};
use compio_fs::File;
use jdb_base::Pos;
use jdb_fs::{HEAD_CRC, HEAD_TOTAL, Head, Load, MAGIC, id_path, open_read};
use log::warn;

use super::{
  WalConf, WalEntry, WalInner,
  consts::{HEADER_SIZE, MIN_FILE_SIZE, SCAN_BUF_SIZE},
  header::{HeaderState, check_header},
};
use crate::{Result, WalPtr, error::Error};

impl<C: WalConf> WalInner<C> {
  async fn open_next_file(
    wal_dir: &std::path::PathBuf,
    file_ids: &Vec<u64>,
    file_idx: &mut usize,
    file: &mut Option<File>,
    cur_id: &mut u64,
    pos: &mut u64,
    len: &mut u64,
    start_offset: u64,
  ) -> Result<bool> {
    while *file_idx < file_ids.len() {
      let id = file_ids[*file_idx];
      let is_first = *file_idx == 0;
      *file_idx += 1;

      *pos = if is_first { start_offset } else { HEADER_SIZE as u64 };

      let path = id_path(wal_dir, id);
      let f = match open_read(&path).await {
        Ok(f) => f,
        Err(_) => continue,
      };

      let meta = match f.metadata().await {
        Ok(m) => m,
        Err(_) => continue,
      };

      *len = meta.len();
      if *len < MIN_FILE_SIZE as u64 {
        warn!("WAL too small: {path:?}, len={len}");
        continue;
      }

      let mut header_buf = vec![0u8; HEADER_SIZE];
      let slice = header_buf.slice(0..HEADER_SIZE);
      let res = f.read_exact_at(slice, 0).await;
      if res.0.is_err() {
        continue;
      }
      header_buf = res.1.into_inner();

      if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
        warn!("WAL header invalid: {path:?}");
        continue;
      }

      *file = Some(f);
      *cur_id = id;
      return Ok(true);
    }

    Ok(false)
  }

  #[allow(clippy::uninit_vec)]
  async fn read_entry(
    file: &mut File,
    buf: &mut Vec<u8>,
    buf_pos: &mut u64,
    buf_cap: &mut usize,
    pos: u64,
    len: u64,
    cur_id: u64,
  ) -> Result<Option<(Head, Vec<u8>, Pos)>> {
    let mut off = (pos - *buf_pos) as usize;
    if off + HEAD_TOTAL > *buf_cap {
      if pos + HEAD_TOTAL as u64 > len {
        return Ok(None);
      }

      buf.clear();
      if buf.capacity() < SCAN_BUF_SIZE {
        buf.reserve(SCAN_BUF_SIZE - buf.capacity());
      }
      unsafe { buf.set_len(SCAN_BUF_SIZE) };

      let read_len = (len - pos).min(SCAN_BUF_SIZE as u64) as usize;
      let tmp = std::mem::take(buf);
      let slice = tmp.slice(0..read_len);
      let res = file.read_at(slice, pos).await;
      *buf = res.1.into_inner();
      let n = res.0?;

      *buf_pos = pos;
      *buf_cap = n;
      off = 0;

      if n < HEAD_TOTAL {
        return Ok(None);
      }
    }

    if unsafe { *buf.get_unchecked(off) } != MAGIC {
      return Err(Error::InvalidMagic);
    }

    let head_pos = pos + 1;
    let head = Head::parse(unsafe { buf.get_unchecked(off + 1..) }, 0, head_pos)?;
    let disk_size = 1 + head.record_size();

    if off + disk_size > *buf_cap {
      if pos + disk_size as u64 > len {
        return Ok(None);
      }

      let need = disk_size.max(SCAN_BUF_SIZE);
      buf.clear();
      if buf.capacity() < need {
        buf.reserve(need - buf.capacity());
      }
      unsafe { buf.set_len(need) };

      let read_len = (len - pos).min(need as u64) as usize;
      let tmp = std::mem::take(buf);
      let slice = tmp.slice(0..read_len);
      let res = file.read_at(slice, pos).await;
      *buf = res.1.into_inner();
      let n = res.0?;

      *buf_pos = pos;
      *buf_cap = n;
      off = 0;

      if n < disk_size {
        return Ok(None);
      }
    }

    let record = unsafe { buf.get_unchecked(off + 1..off + disk_size) };
    let key = head.key_data(record).to_vec();

    let flag = head.flag();
    let p = if flag.is_tombstone() {
      Pos::tombstone(cur_id, head_pos + HEAD_CRC as u64)
    } else if flag.is_infile() {
      Pos::infile_with_flag(flag, cur_id, head_pos + HEAD_CRC as u64, head.val_len)
    } else {
      Pos::file_with_flag(flag, cur_id, head.val_file_id, head.val_len)
    };

    Ok(Some((head, key, p)))
  }

  #[allow(clippy::uninit_vec)]
  async fn search_forward_magic(
    file: &mut File,
    buf: &mut Vec<u8>,
    pos: u64,
    len: u64,
  ) -> Option<u64> {
    let mut search_pos = pos + 1;

    while search_pos < len {
      let read_len = (len - search_pos).min(SCAN_BUF_SIZE as u64) as usize;
      if read_len == 0 {
        break;
      }

      buf.clear();
      if buf.capacity() < read_len {
        buf.reserve(read_len - buf.capacity());
      }
      unsafe { buf.set_len(read_len) };

      let tmp = std::mem::take(buf);
      let slice = tmp.slice(0..read_len);
      let res = file.read_at(slice, search_pos).await;
      *buf = res.1.into_inner();
      if res.0.is_err() {
        break;
      }

      if let Some(idx) = WalEntry::find_magic(buf) {
        let magic_pos = search_pos + idx as u64;

        if magic_pos + HEAD_TOTAL as u64 <= len {
          let head_start = idx + 1;
          if head_start + HEAD_CRC <= buf.len()
            && Head::parse(&buf[head_start..], 0, magic_pos + 1).is_ok()
          {
            return Some(magic_pos);
          }
        }

        search_pos = magic_pos + 1;
      } else {
        search_pos += read_len as u64;
      }
    }

    None
  }
}