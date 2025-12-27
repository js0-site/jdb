//! WAL write operations / WAL 写入操作

use std::fs;

use compio::io::AsyncWriteAtExt;
use compio_fs::OpenOptions;
use zerocopy::IntoBytes;

use super::{Mode, Wal, consts::END_SIZE, end::build_end};
use crate::{
  Head, INFILE_MAX, Pos,
  error::{Error, Result},
  flag::Flag,
};

/// Get key mode by length / 根据长度获取键模式
#[inline]
pub(super) fn key_mode(len: usize) -> Mode {
  if len <= Head::MAX_KEY_INLINE {
    Mode::Inline
  } else if len <= INFILE_MAX {
    Mode::Infile
  } else {
    Mode::File
  }
}

/// Select storage mode by data size / 根据数据大小选择存储模式
#[inline]
pub(super) fn select_mode(key_len: usize, val_len: usize) -> (Mode, Mode) {
  let k_mode = key_mode(key_len);

  let v_mode = if key_len + val_len <= Head::MAX_BOTH_INLINE
    || (val_len <= Head::MAX_VAL_INLINE && k_mode != Mode::Inline)
  {
    Mode::Inline
  } else if val_len <= INFILE_MAX {
    Mode::Infile
  } else {
    Mode::File
  };

  (k_mode, v_mode)
}

impl Wal {
  /// Put key-value with auto mode selection / 自动选择模式写入键值
  pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<Pos> {
    let (k_mode, v_mode) = select_mode(key.len(), val.len());
    let k_len = key.len() as u32;
    let v_len = val.len() as u32;

    let (key_flag, key_pos) = match k_mode {
      Mode::Inline => (Flag::INLINE, Pos::default()),
      Mode::Infile => (Flag::INFILE, self.write_data(key).await?),
      Mode::File => {
        let id = self.gen_id.next_id();
        self.write_file(id, key).await?;
        (Flag::FILE, Pos::new(id, 0))
      }
    };

    let (val_flag, val_pos, val_crc) = match v_mode {
      Mode::Inline => (Flag::INLINE, Pos::default(), 0),
      Mode::Infile => {
        let crc = crc32fast::hash(val);
        let loc = self.write_data(val).await?;
        (Flag::INFILE, loc, crc)
      }
      Mode::File => {
        let crc = crc32fast::hash(val);
        let id = self.gen_id.next_id();
        self.write_file(id, val).await?;
        (Flag::FILE, Pos::new(id, 0), crc)
      }
    };

    let head = match (k_mode, v_mode) {
      (Mode::Inline, Mode::Inline) => Head::both_inline(key, val)?,
      (Mode::Inline, _) => Head::key_inline(key, val_flag, val_pos, v_len, val_crc)?,
      (_, Mode::Inline) => Head::val_inline(key_flag, key_pos, k_len, val)?,
      (..) => Head::both_file(key_flag, key_pos, k_len, val_flag, val_pos, v_len, val_crc)?,
    };

    self.write_head(&head).await
  }

  pub(super) async fn write_head(&mut self, head: &Head) -> Result<Pos> {
    // Check space for head + end marker / 检查空间包含尾部标记
    if self.cur_pos + Head::SIZE as u64 + END_SIZE as u64 > self.max_size {
      self.rotate().await?;
    }

    let file = self.cur_file.as_mut().ok_or(Error::NotOpen)?;
    let head_pos = self.cur_pos;

    // Reuse scratch buffer to avoid allocation / 复用 scratch 缓冲区避免分配
    let mut buf = std::mem::take(&mut self.scratch);
    buf.clear();
    buf.extend_from_slice(head.as_bytes());
    buf.extend_from_slice(&build_end(head_pos));

    let res = file.write_all_at(buf, head_pos).await;
    self.scratch = res.1;
    res.0?;

    self.cur_pos += (Head::SIZE + END_SIZE) as u64;
    Ok(Pos::new(self.cur_id, head_pos))
  }

  pub(super) async fn write_data(&mut self, data: &[u8]) -> Result<Pos> {
    let len = data.len() as u64;
    if self.cur_pos + len > self.max_size {
      self.rotate().await?;
    }

    let file = self.cur_file.as_mut().ok_or(Error::NotOpen)?;
    let pos = self.cur_pos;

    file.write_all_at(data.to_vec(), pos).await.0?;
    self.cur_pos += len;
    Ok(Pos::new(self.cur_id, pos))
  }

  pub(super) async fn write_file(&self, id: u64, data: &[u8]) -> Result<()> {
    let path = self.bin_path(id);
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(data.to_vec(), 0).await.0?;
    file.sync_all().await?;
    Ok(())
  }

  /// Put with existing file id (for GC, no file copy) / 使用已有文件 ID 写入（用于 GC，无需复制文件）
  pub async fn put_with_file(
    &mut self,
    key: &[u8],
    val_file_id: u64,
    val_len: u32,
    val_crc32: u32,
  ) -> Result<Pos> {
    let k_len = key.len();
    let k_mode = key_mode(k_len);

    let (key_flag, key_pos) = match k_mode {
      Mode::Inline => (Flag::INLINE, Pos::default()),
      Mode::Infile => (Flag::INFILE, self.write_data(key).await?),
      Mode::File => {
        let id = self.gen_id.next_id();
        self.write_file(id, key).await?;
        (Flag::FILE, Pos::new(id, 0))
      }
    };

    let head = if k_mode == Mode::Inline {
      Head::key_inline(
        key,
        Flag::FILE,
        Pos::new(val_file_id, 0),
        val_len,
        val_crc32,
      )?
    } else {
      Head::both_file(
        key_flag,
        key_pos,
        k_len as u32,
        Flag::FILE,
        Pos::new(val_file_id, 0),
        val_len,
        val_crc32,
      )?
    };

    self.write_head(&head).await
  }
}
