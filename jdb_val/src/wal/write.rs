//! WAL write operations / WAL 写入操作

use std::mem;

use compio::{buf::IoBuf, io::AsyncWriteAtExt};
use compio_fs::OpenOptions;
use zerocopy::IntoBytes;

use super::{
  MAX_SLOT_SIZE, Mode, WalConf, WalInner,
  consts::{MAGIC_BYTES, MAGIC_SIZE},
};
use crate::{
  Bin, Head, INFILE_MAX, Pos,
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

/// Val info for write_record / write_record 的 val 信息
struct ValInfo {
  flag: Flag,
  pos: Pos,
  len: u32,
  crc: u32,
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
  (MAGIC_SIZE + write_k + write_v + Head::SIZE) as u64
}

impl<C: WalConf> WalInner<C> {
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
    let _ = self.write_file_io(id, key.to_vec()).await?;
    Ok(Pos::new(id, 0))
  }

  /// Write key IO buffer to external file / 写入 key IO 缓冲区到外部文件
  async fn write_key_file_io<T: IoBuf>(&mut self, key: T) -> Result<Pos> {
    let id = self.ider.get();
    let _ = self.write_file_io(id, key).await?;
    Ok(Pos::new(id, 0))
  }

  /// Write IO buffer to external file / 写入 IO 缓冲区到外部文件
  async fn write_file_io<T: IoBuf>(&mut self, id: u64, data: T) -> Result<T> {
    let path = self.bin_path(id);
    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;
    let res = file.write_all_at(data, 0).await;
    res.0?;
    Ok(res.1)
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
    // Layout: [Magic, Head, Key, Val] / 布局：[Magic, Head, Key, Val]
    self
      .write_combined(&[&MAGIC_BYTES, head.as_bytes(), k_data, v_data], start)
      .await?;
    self.cur_pos += total;
    // Pos points to Head (after Magic) / Pos 指向 Head（Magic 之后）
    Ok(Pos::new(self.cur_id, start + MAGIC_SIZE as u64))
  }

  /// Internal: Write record with determined modes and val info
  /// 内部：写入已确定模式和值信息的记录
  async fn write_record(
    &mut self,
    key: &[u8],
    val: &[u8],
    k_mode: Mode,
    v_mode: Mode,
    vi: ValInfo,
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
    // Layout: [Magic, Head, Key, Val] / 布局：[Magic, Head, Key, Val]
    let start = self.cur_pos;
    let mut off = start + (MAGIC_SIZE + Head::SIZE) as u64;

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
      vi.pos
    };

    // 4. Construct Head / 构建 Head
    let head = match (k_mode, v_mode) {
      (Mode::Inline, Mode::Inline) => Head::both_inline(key, val)?,
      (Mode::Inline, _) => Head::key_inline(key, vi.flag, final_val_pos, vi.len, vi.crc)?,
      (_, Mode::Inline) => Head::val_inline(key_flag, final_key_pos, k_len as u32, val)?,
      (..) => Head::both_file(
        key_flag,
        final_key_pos,
        k_len as u32,
        vi.flag,
        final_val_pos,
        vi.len,
        vi.crc,
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

  /// Internal: Write record when key already in file / 内部：key 已写入文件时写入记录
  async fn write_record_file_key(
    &mut self,
    k_len: usize,
    key_pos: Pos,
    vi: ValInfo,
  ) -> Result<Pos> {
    let total = calc_wal_len(Mode::File, Mode::File, 0, 0);

    self.reserve(total).await?;
    let start = self.cur_pos;

    let head = Head::both_file(
      Flag::FILE,
      key_pos,
      k_len as u32,
      vi.flag,
      vi.pos,
      vi.len,
      vi.crc,
    )?;

    self.finalize(start, total, head, &[], &[]).await
  }

  /// Put key-value with auto mode selection / 自动选择模式写入键值
  pub async fn put<'a, 'b>(&mut self, key: impl Bin<'a>, val: impl Bin<'b>) -> Result<Pos> {
    let k_len = key.len();
    let v_len = val.len();
    if k_len > u32::MAX as usize {
      return Err(Error::KeyTooLong(k_len, u32::MAX as usize));
    }
    if v_len > u32::MAX as usize {
      return Err(Error::ValTooLong(v_len, u32::MAX as usize));
    }

    let (k_mode, v_mode) = select_mode(k_len, v_len);

    if v_mode == Mode::File {
      // Zero-copy if val is owned / 如果 val 拥有所有权则零拷贝
      let io = val.io();
      let crc = crc32fast::hash(io.as_slice());
      let id = self.ider.get();
      let _ = self.write_file_io(id, io).await?;
      let vi = ValInfo {
        flag: Flag::FILE,
        pos: Pos::new(id, 0),
        len: v_len as u32,
        crc,
      };
      self
        .write_record(key.as_slice(), &[], k_mode, v_mode, vi)
        .await
    } else if v_mode == Mode::Infile {
      let vi = ValInfo {
        flag: Flag::INFILE,
        pos: Pos::default(),
        len: v_len as u32,
        crc: 0,
      };
      self
        .write_record(key.as_slice(), val.as_slice(), k_mode, v_mode, vi)
        .await
    } else {
      // Inline mode / 内联模式
      let vi = ValInfo {
        flag: Flag::from(v_mode),
        pos: Pos::default(),
        len: v_len as u32,
        crc: 0,
      };
      self
        .write_record(key.as_slice(), val.as_slice(), k_mode, v_mode, vi)
        .await
    }
  }

  /// Put with existing file id (for GC, no file copy) / 使用已有文件 ID 写入
  pub async fn put_with_file<'a>(
    &mut self,
    key: impl Bin<'a>,
    val_flag: Flag,
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
    let vi = ValInfo {
      flag: val_flag,
      pos: Pos::new(val_file_id, 0),
      len: val_len,
      crc: val_crc32,
    };

    // For FILE mode key, use zero-copy / FILE 模式 key 使用零拷贝
    if k_mode == Mode::File {
      let key_pos = self.write_key_file_io(key.io()).await?;
      self.write_record_file_key(k_len, key_pos, vi).await
    } else {
      self
        .write_record(key.as_slice(), &[], k_mode, Mode::File, vi)
        .await
    }
  }

  /// Write multiple parts via queue / 通过队列写入多个部分
  pub(crate) async fn write_combined(&mut self, parts: &[&[u8]], pos: u64) -> Result<()> {
    let total_len: usize = parts.iter().map(|p| p.len()).sum();

    if total_len > MAX_SLOT_SIZE {
      // Fallback: direct write for large data / 回退：大数据直接写入
      self.flush().await?;
      if let Some(f) = self.shared.file() {
        // Build data in scratch buffer / 在临时缓冲区构建数据
        self.data_buf.clear();
        self.data_buf.reserve(total_len);
        for p in parts {
          self.data_buf.extend_from_slice(p);
        }
        let buf = mem::take(&mut self.data_buf);
        let res = f.write_all_at(buf, pos).await;
        self.data_buf = res.1;
        res.0?;
      }
      return Ok(());
    }

    // Push slices directly to queue (avoid double copy)
    // 直接推入切片到队列（避免双重拷贝）
    self.shared.push_slices(pos, parts, total_len);
    self.maybe_spawn_writer();
    Ok(())
  }

  /// Delete key (write tombstone) / 删除键（写入删除标记）
  pub async fn del<'a>(&mut self, key: impl Bin<'a>) -> Result<Pos> {
    let k_len = key.len();
    if k_len > u32::MAX as usize {
      return Err(Error::KeyTooLong(k_len, u32::MAX as usize));
    }

    let base_len = (MAGIC_SIZE + Head::SIZE) as u64;

    // Inline key tombstone / 内联键删除标记
    if k_len <= Head::MAX_BOTH_INLINE {
      self.reserve(base_len).await?;
      let start = self.cur_pos;
      let head = Head::tombstone(key.as_slice())?;
      self.finalize(start, base_len, head, &[], &[]).await
    } else if k_len <= INFILE_MAX {
      // Infile key tombstone / 同文件键删除标记
      let total = base_len + k_len as u64;
      self.reserve(total).await?;
      let start = self.cur_pos;
      // Key pos: after Magic + Head / Key 位置：Magic + Head 之后
      let key_pos = Pos::new(self.cur_id, start + (MAGIC_SIZE + Head::SIZE) as u64);
      let head = Head::tombstone_file(Flag::INFILE, key_pos, k_len as u32)?;
      self.finalize(start, total, head, key.as_slice(), &[]).await
    } else {
      // File key tombstone / 外部文件键删除标记
      self.reserve(base_len).await?;
      let key_pos = self.write_key_file_io(key.io()).await?;
      let start = self.cur_pos;
      let head = Head::tombstone_file(Flag::FILE, key_pos, k_len as u32)?;
      self.finalize(start, base_len, head, &[], &[]).await
    }
  }
}
