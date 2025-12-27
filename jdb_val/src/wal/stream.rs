//! WAL streaming API / WAL 流式接口

use std::fs;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};

use super::{Mode, Wal};
use crate::{Head, INFILE_MAX, Pos, Result, flag::Flag};

impl Wal {
  /// Default chunk size for streaming (64KB) / 流式读写默认块大小
  pub(super) const CHUNK_SIZE: usize = 64 * 1024;

  /// Put with streaming value / 流式写入值
  ///
  /// For large values, writes to separate bin file in chunks.
  /// 对于大值，分块写入独立 bin 文件。
  pub async fn put_stream<I>(&mut self, key: &[u8], val_iter: I) -> Result<Pos>
  where
    I: IntoIterator<Item = Vec<u8>>,
  {
    let k_len = key.len();
    let k_mode = if k_len <= Head::MAX_KEY_INLINE {
      Mode::Inline
    } else if k_len <= INFILE_MAX {
      Mode::Infile
    } else {
      Mode::File
    };

    let (key_flag, key_pos) = match k_mode {
      Mode::Inline => (Flag::INLINE, Pos::default()),
      Mode::Infile => (Flag::INFILE, self.write_data(key).await?),
      Mode::File => {
        let id = self.gen_id.id();
        self.write_file(id, key).await?;
        (Flag::FILE, Pos::new(id, 0))
      }
    };

    // Stream value to bin file / 流式写入值到 bin 文件
    let val_id = self.gen_id.id();
    let (val_len, val_crc) = self.write_file_stream(val_id, val_iter).await?;

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

  /// Write file in streaming mode / 流式写入文件
  ///
  /// Returns (total_len, crc32) / 返回 (总长度, crc32)
  async fn write_file_stream<I>(&self, id: u64, iter: I) -> Result<(u32, u32)>
  where
    I: IntoIterator<Item = Vec<u8>>,
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

    let mut pos = 0u64;
    let mut hasher = crc32fast::Hasher::new();

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
        let buf = vec![0u8; chunk_size];

        let res = file.read_exact_at(buf, *start).await;
        res.0?;
        *start += chunk_size as u64;
        *len -= chunk_size as u64;

        Ok(Some(res.1))
      }
    }
  }

  /// Read all remaining data / 读取所有剩余数据
  pub async fn read_all(&mut self) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    while let Some(chunk) = self.next().await? {
      result.extend(chunk);
    }
    Ok(result)
  }
}
