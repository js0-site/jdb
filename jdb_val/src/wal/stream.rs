//! WAL streaming API / WAL 流式接口

use std::fs;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};

use super::{Mode, Wal, write::key_mode};
use crate::{Head, INFILE_MAX, Pos, Result, flag::Flag};

impl Wal {
  /// Default chunk size for streaming (64KB) / 流式读写默认块大小
  pub(super) const CHUNK_SIZE: usize = 64 * 1024;

  /// First chunk size for mode detection (INFILE_MAX + CHUNK_SIZE, 64KB aligned)
  /// 首次读取大小用于模式检测（INFILE_MAX + CHUNK_SIZE，64KB 对齐）
  const FIRST_CHUNK_SIZE: usize = INFILE_MAX + Self::CHUNK_SIZE;

  /// Put with streaming value / 流式写入值
  ///
  /// First read uses INFILE_MAX + CHUNK_SIZE buffer to detect storage mode:
  /// 首次读取使用 INFILE_MAX + CHUNK_SIZE 大小的 buffer 来检测存储模式：
  /// - If total <= inline limit: store inline / 若总大小 <= 内联限制：内联存储
  /// - If total <= INFILE_MAX: store in WAL file / 若总大小 <= INFILE_MAX：存入 WAL 文件
  /// - Otherwise: store in separate bin file / 否则：存入独立 bin 文件
  pub async fn put_stream<I>(&mut self, key: &[u8], mut val_iter: I) -> Result<Pos>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    // Collect first chunk to detect mode / 收集首块数据以检测模式
    let mut first_buf = Vec::with_capacity(Self::FIRST_CHUNK_SIZE);
    let mut has_more = false;

    for chunk in val_iter.by_ref() {
      let remain = Self::FIRST_CHUNK_SIZE - first_buf.len();
      if chunk.len() <= remain {
        first_buf.extend_from_slice(&chunk);
      } else {
        // Exceeds threshold, must use FILE mode / 超过阈值，必须用 FILE 模式
        first_buf.extend_from_slice(&chunk[..remain]);
        has_more = true;
        break;
      }
      if first_buf.len() >= Self::FIRST_CHUNK_SIZE {
        has_more = true;
        break;
      }
    }

    // Determine mode based on first chunk / 根据首块数据确定模式
    if !has_more && first_buf.len() <= INFILE_MAX {
      // Small enough for inline/infile, use normal put / 足够小，用普通 put
      return self.put(key, &first_buf).await;
    }

    // Large value, must use FILE mode / 大值，必须用 FILE 模式
    let k_len = key.len();
    let k_mode = key_mode(k_len);
    let (key_flag, key_pos) = self.write_key_part(key, k_mode).await?;

    // Stream value to bin file / 流式写入值到 bin 文件
    let val_id = self.gen_id.next_id();
    let (val_len, val_crc) = self
      .write_file_stream_with_first(val_id, first_buf, val_iter)
      .await?;

    let head = if k_mode == Mode::Inline {
      Head::key_inline(key, Flag::FILE, Pos::new(val_id, 0), val_len, val_crc)?
    } else {
      Head::both_file(
        key_flag,
        key_pos,
        k_len as u32,
        Flag::FILE,
        Pos::new(val_id, 0),
        val_len,
        val_crc,
      )?
    };

    self.write_head(&head).await
  }

  /// Write file in streaming mode with pre-read first chunk
  /// 流式写入文件，带预读的首块数据
  ///
  /// Returns (total_len, crc32) / 返回 (总长度, crc32)
  async fn write_file_stream_with_first<I>(
    &self,
    id: u64,
    first: Vec<u8>,
    iter: I,
  ) -> Result<(u32, u32)>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    let path = self.bin_path(id);
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    let mut hasher = crc32fast::Hasher::new();

    // Write first chunk / 写入首块
    hasher.update(&first);
    let mut pos = first.len() as u64;
    file.write_all_at(first, 0).await.0?;

    // Write remaining chunks / 写入剩余块
    for chunk in iter {
      hasher.update(&chunk);
      let len = chunk.len() as u64;
      file.write_all_at(chunk, pos).await.0?;
      pos += len;
    }

    file.sync_all().await?;
    Ok((pos as u32, hasher.finalize()))
  }

  /// Read key in streaming mode / 流式读取键
  pub async fn head_key_stream(&self, head: &Head) -> Result<DataStream> {
    if head.key_flag.is_inline() {
      return Ok(DataStream::Inline(head.key_data().to_vec()));
    }

    let loc = head.key_pos();
    let len = head.key_len.get() as u64;

    self.open_stream(head.key_flag, loc, len).await
  }

  /// Read value in streaming mode / 流式读取值
  pub async fn head_val_stream(&self, head: &Head) -> Result<DataStream> {
    if head.val_flag.is_inline() {
      return Ok(DataStream::Inline(head.val_data().to_vec()));
    }

    let loc = head.val_pos();
    let len = head.val_len.get() as u64;

    self.open_stream(head.val_flag, loc, len).await
  }

  /// Open data stream by flag and location / 根据标志和位置打开数据流
  async fn open_stream(&self, flag: Flag, loc: Pos, len: u64) -> Result<DataStream> {
    if flag.is_file() {
      let path = self.bin_path(loc.id());
      let file = OpenOptions::new().read(true).open(&path).await?;
      Ok(DataStream::File {
        file,
        start: 0,
        len,
      })
    } else {
      // Infile: read from WAL / 文件内：从 WAL 读取
      let path = self.wal_path(loc.id());
      let file = OpenOptions::new().read(true).open(&path).await?;
      Ok(DataStream::File {
        file,
        start: loc.pos(),
        len,
      })
    }
  }
}

/// Data stream for reading large data / 用于读取大数据的流
pub enum DataStream {
  /// Inline data / 内联数据
  Inline(Vec<u8>),
  /// File-based stream / 基于文件的流
  File { file: File, start: u64, len: u64 },
}

impl DataStream {
  /// Read next chunk / 读取下一块
  ///
  /// Returns None when finished / 结束时返回 None
  #[allow(clippy::uninit_vec)]
  pub async fn next(&mut self) -> Result<Option<Vec<u8>>> {
    match self {
      DataStream::Inline(data) => {
        if data.is_empty() {
          Ok(None)
        } else {
          Ok(Some(std::mem::take(data)))
        }
      }
      DataStream::File { file, start, len } => {
        if *len == 0 {
          return Ok(None);
        }

        let chunk_size = (*len as usize).min(Wal::CHUNK_SIZE);
        // Optimization: Avoid zero-initialization / 优化：避免零初始化
        let mut buf = Vec::with_capacity(chunk_size);
        // SAFETY: read_exact_at will overwrite the buffer / read_exact_at 会覆盖缓冲区
        unsafe { buf.set_len(chunk_size) };

        let res = file.read_exact_at(buf, *start).await;
        res.0?;
        *start += chunk_size as u64;
        *len -= chunk_size as u64;

        Ok(Some(res.1))
      }
    }
  }

  /// Read all remaining data / 读取所有剩余数据
  #[allow(clippy::uninit_vec)]
  pub async fn read_all(&mut self) -> Result<Vec<u8>> {
    match self {
      DataStream::Inline(data) => Ok(std::mem::take(data)),
      DataStream::File { file, start, len } => {
        let size = *len as usize;
        let mut buf = Vec::with_capacity(size);
        // SAFETY: read_exact_at guarantees filling the buffer
        // 安全：read_exact_at 保证填满缓冲区
        unsafe { buf.set_len(size) };
        let res = file.read_exact_at(buf, *start).await;
        res.0?;
        // Mark as consumed / 标记为已消费
        *start += *len;
        *len = 0;
        Ok(res.1)
      }
    }
  }
}
