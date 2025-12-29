//! WAL streaming API
//! WAL 流式接口

use std::mem;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::{File, OpenOptions};

use super::{Wal, WalConf, WalInner, lz4};
use crate::{Bin, FilePos, Head, INFILE_MAX, Pos, Result, Store};

impl<C: WalConf> WalInner<C> {
  /// Default chunk size for streaming (64KB)
  /// 流式读写默认块大小
  pub(super) const CHUNK_SIZE: usize = 64 * 1024;

  /// First chunk size for mode detection
  /// 首次读取大小用于模式检测
  const FIRST_CHUNK_SIZE: usize = INFILE_MAX + Self::CHUNK_SIZE;

  /// Put with streaming value
  /// 流式写入值
  pub async fn put_stream<'a, I>(&mut self, key: impl Bin<'a>, mut val_iter: I) -> Result<Pos>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    let mut first_buf = mem::take(&mut self.data_buf);
    first_buf.clear();
    first_buf.reserve(Self::FIRST_CHUNK_SIZE);

    let mut has_more = false;
    let mut leftover: Option<Vec<u8>> = None;
    for mut chunk in val_iter.by_ref() {
      let remain = Self::FIRST_CHUNK_SIZE - first_buf.len();
      if chunk.len() <= remain {
        first_buf.extend(chunk);
        if first_buf.len() >= Self::FIRST_CHUNK_SIZE {
          has_more = true;
          break;
        }
      } else {
        first_buf.extend_from_slice(&chunk[..remain]);
        chunk.drain(..remain);
        leftover = Some(chunk);
        has_more = true;
        break;
      }
    }

    if !has_more && first_buf.len() <= INFILE_MAX {
      let res = self.put(key, &first_buf).await;
      self.data_buf = first_buf;
      return res;
    }

    // Large value, use FILE mode
    // 大值，使用 FILE 模式
    let val_id = self.ider.get();
    let combined_iter = leftover.into_iter().chain(val_iter);
    let (val_len, val_hash) = self
      .write_file_stream_with_first(val_id, first_buf, combined_iter)
      .await?;

    self
      .put_with_file(key, Store::File, val_id, val_len, val_hash)
      .await
  }

  /// Write file in streaming mode with pre-read first chunk
  /// 流式写入文件（带预读首块）
  async fn write_file_stream_with_first<I>(
    &mut self,
    id: u64,
    first: Vec<u8>,
    iter: I,
  ) -> Result<(u64, u128)>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    use std::hash::Hasher;

    use gxhash::GxHasher;

    let path = self.bin_path(id);

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    let mut hasher = GxHasher::with_seed(0);
    hasher.write(&first);

    let mut pos = first.len() as u64;
    let res = file.write_all_at(first, 0).await;
    res.0?;

    let mut buf = res.1;
    let cap = buf.capacity();
    buf.clear();

    for chunk in iter {
      hasher.write(&chunk);

      if buf.len() + chunk.len() > cap && !buf.is_empty() {
        let len = buf.len();
        let res = file.write_all_at(buf, pos).await;
        res.0?;
        pos += len as u64;
        buf = res.1;
        buf.clear();
      }

      if chunk.len() > cap {
        let len = chunk.len();
        file.write_all_at(chunk, pos).await.0?;
        pos += len as u64;
      } else {
        buf.extend(chunk);
      }
    }

    if !buf.is_empty() {
      let len = buf.len();
      let res = file.write_all_at(buf, pos).await;
      res.0?;
      pos += len as u64;
      buf = res.1;
    }

    self.data_buf = buf;

    Ok((pos, hasher.finish_u128()))
  }

  /// Read key in streaming mode
  /// 流式读取键
  pub async fn key_stream(&self, head: &Head, head_data: &[u8]) -> Result<DataStream> {
    let store = head.key_store();
    if store.is_infile() {
      let key_data = head.key_data(head_data);
      if store.is_lz4() {
        let mut buf = Vec::new();
        lz4::decompress(key_data, head.key_len as usize, &mut buf)?;
        return Ok(DataStream::Inline(buf));
      }
      return Ok(DataStream::Inline(key_data.to_vec()));
    }

    let fpos = head.key_file_pos(head_data);
    self.open_stream(fpos, head.key_len, store).await
  }

  /// Read value in streaming mode
  /// 流式读取值
  pub async fn val_stream(&self, head: &Head, head_data: &[u8]) -> Result<DataStream> {
    if head.is_tombstone() {
      return Ok(DataStream::Inline(Vec::new()));
    }

    let store = head.val_store();
    let val_len = head.val_len.unwrap_or(0);

    if store.is_infile() {
      let val_data = head.val_data(head_data);
      if store.is_lz4() {
        let mut buf = Vec::new();
        lz4::decompress(val_data, val_len as usize, &mut buf)?;
        return Ok(DataStream::Inline(buf));
      }
      return Ok(DataStream::Inline(val_data.to_vec()));
    }

    let fpos = head.val_file_pos(head_data);
    self.open_stream(fpos, val_len, store).await
  }

  /// Open data stream
  /// 打开数据流
  async fn open_stream(&self, fpos: FilePos, len: u64, store: Store) -> Result<DataStream> {
    let path = self.bin_path(fpos.file_id);
    let file = OpenOptions::new().read(true).open(&path).await?;

    // LZ4: read all and decompress
    // LZ4: 完整读取并解压缩
    if store.is_lz4() {
      let file_len = file.metadata().await?.len() as usize;
      let buf = vec![0u8; file_len];
      let res = file.read_exact_at(buf.slice(0..file_len), 0).await;
      res.0?;
      let compressed = res.1.into_inner();

      if gxhash::gxhash128(&compressed, 0) != fpos.hash {
        return Err(crate::Error::HashMismatch);
      }

      let mut decompressed = Vec::new();
      lz4::decompress(&compressed, len as usize, &mut decompressed)?;
      return Ok(DataStream::Inline(decompressed));
    }

    Ok(DataStream::File {
      file,
      pos: fpos.offset,
      remain: len,
      hash: fpos.hash,
      buf: Vec::new(),
    })
  }
}

/// Data stream for reading large data
/// 用于读取大数据的流
pub enum DataStream {
  /// Inline data
  /// 内联数据
  Inline(Vec<u8>),
  /// File-based stream
  /// 基于文件的流
  File {
    file: File,
    pos: u64,
    remain: u64,
    hash: u128,
    buf: Vec<u8>,
  },
}

impl DataStream {
  /// Read next chunk
  /// 读取下一块
  pub async fn next(&mut self) -> Result<Option<Vec<u8>>> {
    match self {
      DataStream::Inline(data) => {
        if data.is_empty() {
          Ok(None)
        } else {
          Ok(Some(mem::take(data)))
        }
      }
      DataStream::File {
        file,
        pos,
        remain,
        buf,
        ..
      } => {
        if *remain == 0 {
          return Ok(None);
        }
        let size = (*remain).min(Wal::CHUNK_SIZE as u64) as usize;
        let tmp = Wal::prepare_buf(buf, size);
        let res = file.read_exact_at(tmp.slice(0..size), *pos).await;
        res.0?;
        *pos += size as u64;
        *remain -= size as u64;
        Ok(Some(res.1.into_inner()))
      }
    }
  }

  /// Read all remaining data
  /// 读取所有剩余数据
  pub async fn read_all(&mut self) -> Result<Vec<u8>> {
    match self {
      DataStream::Inline(data) => Ok(mem::take(data)),
      DataStream::File {
        file,
        pos,
        remain,
        hash,
        buf,
      } => {
        let size = *remain as usize;
        let tmp = Wal::prepare_buf(buf, size);
        let res = file.read_exact_at(tmp.slice(0..size), *pos).await;
        res.0?;
        *pos += *remain;
        *remain = 0;
        let data = res.1.into_inner();

        // Verify hash
        // 验证哈希
        if gxhash::gxhash128(&data, 0) != *hash {
          return Err(crate::Error::HashMismatch);
        }

        Ok(data)
      }
    }
  }
}
