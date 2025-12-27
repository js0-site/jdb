//! WAL write operations / WAL 写入操作

use std::mem;

use compio::io::AsyncWriteAtExt;
use compio_fs::OpenOptions;
use zerocopy::IntoBytes;

use super::{Mode, Wal, WriteReq, consts::END_SIZE, end::build_end};
use crate::{
  Head, INFILE_MAX, Pos,
  error::{Error, Result},
  flag::Flag,
};

impl From<Mode> for Flag {
  #[inline(always)]
  fn from(m: Mode) -> Self {
    match m {
      Mode::Inline => Flag::INLINE,
      Mode::Infile => Flag::INFILE,
      Mode::File => Flag::FILE,
    }
  }
}

/// Get key mode by length / 根据长度获取键模式
#[inline(always)]
pub(crate) const fn key_mode(len: usize) -> Mode {
  if len <= Head::MAX_KEY_INLINE {
    Mode::Inline
  } else if len <= INFILE_MAX {
    Mode::Infile
  } else {
    Mode::File
  }
}

/// Select storage mode by data size / 根据数据大小选择存储模式
#[inline(always)]
const fn select_mode(key_len: usize, val_len: usize) -> (Mode, Mode) {
  // Optimization: Check both_inline first to save space (e.g. key > 30 but total <= 50)
  // 优化：优先检查 both_inline 以节省空间（例如 key > 30 但总大小 <= 50）
  if key_len + val_len <= Head::MAX_BOTH_INLINE {
    return (Mode::Inline, Mode::Inline);
  }

  let k_mode = key_mode(key_len);

  // v_mode is Inline only if k_mode is NOT Inline (Val uses remaining Head space)
  // v_mode 为 Inline 仅当 k_mode 不是 Inline（Val 使用 Head 剩余空间）
  let v_mode = if val_len <= Head::MAX_VAL_INLINE && !matches!(k_mode, Mode::Inline) {
    Mode::Inline
  } else if val_len <= INFILE_MAX {
    Mode::Infile
  } else {
    Mode::File
  };

  (k_mode, v_mode)
}

/// Calculate WAL record size / 计算 WAL 记录大小
#[inline(always)]
const fn calc_wal_len(k_mode: Mode, v_mode: Mode, k_len: usize, v_len: usize) -> u64 {
  let write_k = if matches!(k_mode, Mode::Infile) {
    k_len
  } else {
    0
  };
  let write_v = if matches!(v_mode, Mode::Infile) {
    v_len
  } else {
    0
  };
  (write_k + write_v + Head::SIZE + END_SIZE) as u64
}

impl Wal {
  /// Reserve WAL space, rotate if needed / 预留 WAL 空间，必要时轮转
  #[inline]
  pub(crate) async fn reserve(&mut self, len: u64) -> Result<()> {
    if self.cur_pos + len > self.max_size {
      self.rotate().await?;
    }
    Ok(())
  }

  /// Write key to external file / 写入 key 到外部文件
  async fn write_key_file(&mut self, key: &[u8]) -> Result<Pos> {
    let id = self.ider.get();
    self.write_file(id, key).await?;
    Ok(Pos::new(id, 0))
  }

  /// Write val to external file / 写入 val 到外部文件
  async fn write_val_file(&mut self, val: &[u8]) -> Result<(Pos, u32)> {
    let crc = crc32fast::hash(val);
    let id = self.ider.get();
    self.write_file(id, val).await?;
    Ok((Pos::new(id, 0), crc))
  }

  /// Finalize record write / 完成记录写入
  async fn finalize(
    &mut self,
    start: u64,
    total: u64,
    head: Head,
    k_data: &[u8],
    v_data: &[u8],
  ) -> Result<Pos> {
    let end = build_end(start);
    // Layout: [Head, Key, Val, End] / 布局：[Head, Key, Val, End]
    self
      .write_combined(&[head.as_bytes(), k_data, v_data, &end], start)
      .await?;
    self.cur_pos += total;
    Ok(Pos::new(self.cur_id, start))
  }

  /// Internal: Write record with determined modes and val info
  /// 内部：写入已确定模式和值信息的记录
  #[allow(clippy::too_many_arguments)]
  async fn write_record(
    &mut self,
    key: &[u8],
    val: &[u8],
    k_mode: Mode,
    v_mode: Mode,
    val_flag: Flag,
    val_pos: Pos,
    val_len: u32,
    val_crc: u32,
  ) -> Result<Pos> {
    let k_len = key.len();
    let total = calc_wal_len(k_mode, v_mode, k_len, val.len());

    // 1. Reserve space / 预留空间
    self.reserve(total).await?;

    // 2. Handle external key file / 处理外部 key 文件
    let (key_flag, key_pos) = if matches!(k_mode, Mode::File) {
      (Flag::FILE, self.write_key_file(key).await?)
    } else {
      (Flag::from(k_mode), Pos::default())
    };

    // 3. Resolve positions in WAL / 解析 WAL 中位置
    // Layout: [Head, Key, Val, End] / 布局：[Head, Key, Val, End]
    let start = self.cur_pos;
    let mut off = start + Head::SIZE as u64;

    let final_key_pos = if matches!(k_mode, Mode::Infile) {
      let p = Pos::new(self.cur_id, off);
      off += k_len as u64;
      p
    } else {
      key_pos
    };

    let final_val_pos = if matches!(v_mode, Mode::Infile) {
      Pos::new(self.cur_id, off)
    } else {
      val_pos
    };

    // 4. Construct Head / 构建 Head
    let head = match (k_mode, v_mode) {
      (Mode::Inline, Mode::Inline) => Head::both_inline(key, val)?,
      (Mode::Inline, _) => Head::key_inline(key, val_flag, final_val_pos, val_len, val_crc)?,
      (_, Mode::Inline) => Head::val_inline(key_flag, final_key_pos, k_len as u32, val)?,
      (..) => Head::both_file(
        key_flag,
        final_key_pos,
        k_len as u32,
        val_flag,
        final_val_pos,
        val_len,
        val_crc,
      )?,
    };

    // 5. Finalize write / 完成写入
    let k_data: &[u8] = if matches!(k_mode, Mode::Infile) {
      key
    } else {
      &[]
    };
    let v_data: &[u8] = if matches!(v_mode, Mode::Infile) {
      val
    } else {
      &[]
    };
    self.finalize(start, total, head, k_data, v_data).await
  }

  /// Put key-value with auto mode selection / 自动选择模式写入键值
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

    let (val_flag, val_pos, val_crc) = if v_mode == Mode::File {
      let (pos, crc) = self.write_val_file(val).await?;
      (Flag::FILE, pos, crc)
    } else {
      (Flag::from(v_mode), Pos::default(), 0)
    };

    self
      .write_record(
        key,
        val,
        k_mode,
        v_mode,
        val_flag,
        val_pos,
        v_len as u32,
        val_crc,
      )
      .await
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
    // Force v_mode to File logic as value is already external
    // 强制 v_mode 为 File 逻辑，因为值已在外部
    self
      .write_record(
        key,
        &[], // val data unused for File mode
        k_mode,
        Mode::File,
        Flag::FILE,
        Pos::new(val_file_id, 0),
        val_len,
        val_crc32,
      )
      .await
  }

  pub(super) async fn write_file(&mut self, id: u64, data: &[u8]) -> Result<()> {
    let path = self.bin_path(id);

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    // Reuse file_buf to avoid allocation / 复用 file_buf 避免分配
    let mut buf = mem::take(&mut self.file_buf);
    buf.clear();
    buf.extend_from_slice(data);

    let res = file.write_all_at(buf, 0).await;
    self.file_buf = res.1;
    res.0?;
    Ok(())
  }

  /// Write multiple parts via background channel / 通过后台通道写入多个部分
  pub(crate) async fn write_combined(&mut self, parts: &[&[u8]], pos: u64) -> Result<()> {
    let total_len: usize = parts.iter().map(|p| p.len()).sum();
    let mut data = Vec::with_capacity(total_len);
    for p in parts {
      data.extend_from_slice(p);
    }

    // Send to background writer / 发送到后台写入器
    self
      .write_tx
      .send(WriteReq { data, pos })
      .await
      .map_err(|_| Error::ChannelClosed)?;
    Ok(())
  }

  /// Delete key (write tombstone) / 删除键（写入删除标记）
  pub async fn del(&mut self, key: &[u8]) -> Result<Pos> {
    let k_len = key.len();
    if k_len > u32::MAX as usize {
      return Err(Error::KeyTooLong(k_len, u32::MAX as usize));
    }

    let base_len = (Head::SIZE + END_SIZE) as u64;

    // Inline key tombstone / 内联键删除标记
    if k_len <= Head::MAX_BOTH_INLINE {
      self.reserve(base_len).await?;
      let start = self.cur_pos;
      let head = Head::tombstone(key)?;
      self.finalize(start, base_len, head, &[], &[]).await
    } else if k_len <= INFILE_MAX {
      // Infile key tombstone / 同文件键删除标记
      let total = base_len + k_len as u64;
      self.reserve(total).await?;
      let start = self.cur_pos;
      let key_pos = Pos::new(self.cur_id, start + Head::SIZE as u64);
      let head = Head::tombstone_file(Flag::INFILE, key_pos, k_len as u32)?;
      self.finalize(start, total, head, key, &[]).await
    } else {
      // File key tombstone / 外部文件键删除标记
      self.reserve(base_len).await?;
      let key_pos = self.write_key_file(key).await?;
      let start = self.cur_pos;
      let head = Head::tombstone_file(Flag::FILE, key_pos, k_len as u32)?;
      self.finalize(start, base_len, head, &[], &[]).await
    }
  }
}
