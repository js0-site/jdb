//! WAL write operations / WAL 写入操作

use compio::io::AsyncWriteAtExt;
use compio_fs::OpenOptions;
use zerocopy::IntoBytes;

use super::{Mode, Wal, consts::END_SIZE, end::build_end};
use crate::{
  Head, INFILE_MAX, Pos,
  error::{Error, Result},
  flag::Flag,
};

/// Extend buffer with multiple parts / 将多个部分追加到缓冲区
#[inline]
fn extend_parts(buf: &mut Vec<u8>, parts: &[&[u8]]) {
  let total: usize = parts.iter().map(|p| p.len()).sum();
  buf.clear();
  buf.reserve(total);
  for p in parts {
    buf.extend_from_slice(p);
  }
}

impl From<Mode> for Flag {
  #[inline]
  fn from(m: Mode) -> Self {
    match m {
      Mode::Inline => Flag::INLINE,
      Mode::Infile => Flag::INFILE,
      Mode::File => Flag::FILE,
    }
  }
}

/// Get key mode by length / 根据长度获取键模式
#[inline]
pub(super) const fn key_mode(len: usize) -> Mode {
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
pub(super) const fn select_mode(key_len: usize, val_len: usize) -> (Mode, Mode) {
  // Optimization: Check both_inline first to save space (e.g. key > 30 but total <= 50)
  // 优化：优先检查 both_inline 以节省空间（例如 key > 30 但总大小 <= 50）
  if key_len + val_len <= Head::MAX_BOTH_INLINE {
    return (Mode::Inline, Mode::Inline);
  }

  let k_mode = key_mode(key_len);

  // If k_mode is Inline but total > 50, v_mode cannot be Inline (handled by top check).
  // So v_mode is Inline only if k_mode is NOT Inline (Val uses remaining Head space).
  // 如果 k_mode 是 Inline 但总大小 > 50，v_mode 不能是 Inline（已由上面检查处理）。
  // 所以 v_mode 为 Inline 仅当 k_mode 不是 Inline（Val 使用 Head 剩余空间）。
  let v_mode = if val_len <= Head::MAX_VAL_INLINE && !matches!(k_mode, Mode::Inline) {
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
  ///
  /// Optimized to coalesce writes into a single IO operation when possible.
  /// 优化：尽可能将多次写入合并为单次 IO 操作。
  pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<Pos> {
    let k_len = key.len();
    let v_len = val.len();
    if k_len > u32::MAX as usize {
      return Err(Error::KeyTooLong(k_len, u32::MAX as usize));
    }
    if v_len > u32::MAX as usize {
      return Err(Error::ValTooLong(v_len, u32::MAX as usize));
    }

    let (k_mode, v_mode) = select_mode(k_len, v_len);

    // 1. Handle Side Effects (Binary Files) / 处理副作用（二进制文件）
    let (key_flag, key_pos) = if k_mode == Mode::File {
      let id = self.gen_id.next_id();
      self.write_file(id, key).await?;
      (Flag::FILE, Pos::new(id, 0))
    } else {
      (Flag::from(k_mode), Pos::default())
    };

    let (val_flag, val_pos, val_crc) = if v_mode == Mode::File {
      let crc = crc32fast::hash(val);
      let id = self.gen_id.next_id();
      self.write_file(id, val).await?;
      (Flag::FILE, Pos::new(id, 0), crc)
    } else {
      (Flag::from(v_mode), Pos::default(), 0)
    };

    // 2. Prepare Atomic WAL Write / 准备原子 WAL 写入
    let write_k_len = if k_mode == Mode::Infile { k_len } else { 0 };
    let write_v_len = if v_mode == Mode::Infile { v_len } else { 0 };
    let total_len = (write_k_len + write_v_len + Head::SIZE + END_SIZE) as u64;

    if self.cur_pos + total_len > self.max_size {
      self.rotate().await?;
    }

    let start_pos = self.cur_pos;
    let mut curr_offset = start_pos;

    // 3. Resolve Infile Positions / 解析 Infile 位置
    let final_key_pos = if k_mode == Mode::Infile {
      let p = Pos::new(self.cur_id, curr_offset);
      curr_offset += write_k_len as u64;
      p
    } else {
      key_pos
    };

    let final_val_pos = if v_mode == Mode::Infile {
      let p = Pos::new(self.cur_id, curr_offset);
      curr_offset += write_v_len as u64;
      p
    } else {
      val_pos
    };

    // 4. Construct Head / 构建 Head
    let head = match (k_mode, v_mode) {
      (Mode::Inline, Mode::Inline) => Head::both_inline(key, val)?,
      (Mode::Inline, _) => Head::key_inline(key, val_flag, final_val_pos, v_len as u32, val_crc)?,
      (_, Mode::Inline) => Head::val_inline(key_flag, final_key_pos, k_len as u32, val)?,
      (..) => Head::both_file(
        key_flag,
        final_key_pos,
        k_len as u32,
        val_flag,
        final_val_pos,
        v_len as u32,
        val_crc,
      )?,
    };

    // 5. Coalesce & Write / 合并并写入
    let end_bytes = build_end(curr_offset);
    let k_data: &[u8] = if k_mode == Mode::Infile { key } else { &[] };
    let v_data: &[u8] = if v_mode == Mode::Infile { val } else { &[] };
    self
      .write_combined(&[k_data, v_data, head.as_bytes(), &end_bytes], start_pos)
      .await?;

    self.cur_pos += total_len;
    Ok(Pos::new(self.cur_id, curr_offset))
  }

  /// Write key logic reused by put_stream
  pub(super) async fn write_key_part(&mut self, key: &[u8], mode: Mode) -> Result<(Flag, Pos)> {
    match mode {
      Mode::Inline => Ok((Flag::INLINE, Pos::default())),
      Mode::Infile => Ok((Flag::INFILE, self.write_data(key).await?)),
      Mode::File => {
        let id = self.gen_id.next_id();
        self.write_file(id, key).await?;
        Ok((Flag::FILE, Pos::new(id, 0)))
      }
    }
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

    // Reuse data_buf to avoid allocation / 复用 data_buf 避免分配
    let mut buf = std::mem::take(&mut self.data_buf);
    buf.clear();
    buf.reserve(data.len());
    buf.extend_from_slice(data);

    let res = file.write_all_at(buf, pos).await;
    self.data_buf = res.1;
    res.0?;

    self.cur_pos += len;
    Ok(Pos::new(self.cur_id, pos))
  }

  pub(super) async fn write_file(&mut self, id: u64, data: &[u8]) -> Result<()> {
    let path = self.bin_path(id);

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    // Reuse file_buf to avoid allocation / 复用 file_buf 避免分配
    let mut buf = std::mem::take(&mut self.file_buf);
    buf.clear();
    buf.reserve(data.len());
    buf.extend_from_slice(data);

    let res = file.write_all_at(buf, 0).await;
    self.file_buf = res.1;
    res.0?;
    file.sync_all().await?;
    Ok(())
  }

  /// Put with existing file id (for GC, no file copy) / 使用已有文件 ID 写入
  pub async fn put_with_file(
    &mut self,
    key: &[u8],
    val_file_id: u64,
    val_len: u32,
    val_crc32: u32,
  ) -> Result<Pos> {
    let k_len = key.len();
    if k_len > u32::MAX as usize {
      return Err(Error::KeyTooLong(k_len, u32::MAX as usize));
    }

    let k_mode = key_mode(k_len);

    // 1. Handle Side Effects
    let (key_flag, key_pos) = if k_mode == Mode::File {
      let id = self.gen_id.next_id();
      self.write_file(id, key).await?;
      (Flag::FILE, Pos::new(id, 0))
    } else {
      (Flag::from(k_mode), Pos::default())
    };

    // 2. Prepare Write
    let write_k_len = if k_mode == Mode::Infile { k_len } else { 0 };
    let total_len = (write_k_len + Head::SIZE + END_SIZE) as u64;

    if self.cur_pos + total_len > self.max_size {
      self.rotate().await?;
    }

    let start_pos = self.cur_pos;
    let mut curr_offset = start_pos;

    // 3. Resolve Position
    let final_key_pos = if k_mode == Mode::Infile {
      let p = Pos::new(self.cur_id, curr_offset);
      curr_offset += write_k_len as u64;
      p
    } else {
      key_pos
    };

    // 4. Construct Head
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
        final_key_pos,
        k_len as u32,
        Flag::FILE,
        Pos::new(val_file_id, 0),
        val_len,
        val_crc32,
      )?
    };

    // 5. Coalesce & Write / 合并并写入
    let end_bytes = build_end(curr_offset);
    let k_data: &[u8] = if k_mode == Mode::Infile { key } else { &[] };
    self
      .write_combined(&[k_data, head.as_bytes(), &end_bytes], start_pos)
      .await?;

    self.cur_pos += total_len;
    Ok(Pos::new(self.cur_id, curr_offset))
  }

  /// Write multiple parts combined into one IO / 将多个部分合并为一次 IO 写入
  async fn write_combined(&mut self, parts: &[&[u8]], pos: u64) -> Result<()> {
    let mut buf = std::mem::take(&mut self.data_buf);
    extend_parts(&mut buf, parts);

    let file = self.cur_file.as_mut().ok_or(Error::NotOpen)?;
    let res = file.write_all_at(buf, pos).await;
    self.data_buf = res.1;
    res.0?;
    Ok(())
  }
}
