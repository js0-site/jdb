//! WAL streaming API
//! WAL 流式接口

use std::mem;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::File;

use super::{Wal, WalConf, WalInner, lz4};
use crate::{Bin, Flag, Head, INFILE_MAX, Result, open_read, open_write_create};

impl<C: WalConf> WalInner<C> {
  pub(super) const CHUNK_SIZE: usize = 64 * 1024;
  const FIRST_CHUNK_SIZE: usize = INFILE_MAX + Self::CHUNK_SIZE;

  /// Put with streaming value
  /// 流式写入值
  pub async fn put_stream<'a, I>(
    &mut self,
    key: impl Bin<'a>,
    mut val_iter: I,
  ) -> Result<crate::Pos>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    let mut first_buf = Vec::with_capacity(Self::FIRST_CHUNK_SIZE);

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
      return self.put(key, &first_buf).await;
    }

    let val_id = self.ider.get();
    let combined_iter = leftover.into_iter().chain(val_iter);
    let val_len = self
      .write_file_stream_with_first(val_id, first_buf, combined_iter)
      .await?;

    self
      .put_with_file(key, Flag::File, val_id, val_len as u32)
      .await
  }

  async fn write_file_stream_with_first<I>(
    &mut self,
    id: u64,
    first: Vec<u8>,
    iter: I,
  ) -> Result<u64>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    let path = self.bin_path(id);
    let mut file = open_write_create(&path).await?;

    let mut pos = first.len() as u64;
    let res = file.write_all_at(first, 0).await;
    res.0?;

    let mut buf = res.1;
    let cap = buf.capacity();
    buf.clear();

    for chunk in iter {
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
      file.write_all_at(buf, pos).await.0?;
      pos += len as u64;
    }

    Ok(pos)
  }

  /// Read value in streaming mode
  /// 流式读取值
  pub async fn val_stream(&self, head: &Head, record: &[u8]) -> Result<DataStream> {
    if head.is_tombstone() {
      return Ok(DataStream::Inline(Vec::new()));
    }

    let store = head.flag();

    if head.val_is_infile() {
      let val = head.val_data(record);
      if store.is_lz4() {
        let mut buf = Vec::new();
        lz4::decompress(val, head.val_len as usize, &mut buf)?;
        return Ok(DataStream::Inline(buf));
      }
      return Ok(DataStream::Inline(val.to_vec()));
    }

    self
      .open_stream(head.val_file_id, head.val_len as u64, store)
      .await
  }

  async fn open_stream(&self, file_id: u64, len: u64, store: Flag) -> Result<DataStream> {
    let path = self.bin_path(file_id);
    let file = open_read(&path).await?;

    if store.is_lz4() {
      let file_len = file.metadata().await?.len() as usize;
      let buf = vec![0u8; file_len];
      let res = file.read_exact_at(buf.slice(0..file_len), 0).await;
      res.0?;
      let compressed = res.1.into_inner();

      let mut decompressed = Vec::new();
      lz4::decompress(&compressed, len as usize, &mut decompressed)?;
      return Ok(DataStream::Inline(decompressed));
    }

    Ok(DataStream::File {
      file,
      pos: 0,
      remain: len,
      buf: Vec::new(),
    })
  }
}

/// Data stream for reading large data
/// 用于读取大数据的流
pub enum DataStream {
  Inline(Vec<u8>),
  File {
    file: File,
    pos: u64,
    remain: u64,
    buf: Vec<u8>,
  },
}

impl DataStream {
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

  pub async fn read_all(&mut self) -> Result<Vec<u8>> {
    match self {
      DataStream::Inline(data) => Ok(mem::take(data)),
      DataStream::File {
        file,
        pos,
        remain,
        buf,
      } => {
        let size = *remain as usize;
        let tmp = Wal::prepare_buf(buf, size);
        let res = file.read_exact_at(tmp.slice(0..size), *pos).await;
        res.0?;
        *pos += *remain;
        *remain = 0;
        Ok(res.1.into_inner())
      }
    }
  }
}
