//! WAL streaming API / WAL 流式接口

use std::mem;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::{File, OpenOptions};
use zerocopy::IntoBytes;

use super::{Mode, Wal, consts::END_SIZE, end::build_end, write::key_mode};
use crate::{Flag, Head, INFILE_MAX, Pos, Result};

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
    // Reuse stream_buf to avoid allocation / 复用 stream_buf 避免分配
    let mut first_buf = mem::take(&mut self.stream_buf);
    first_buf.clear();
    first_buf.reserve(Self::FIRST_CHUNK_SIZE);

    let mut has_more = false;
    let mut leftover: Option<Vec<u8>> = None; // Save remaining part of split chunk / 保存分割块的剩余部分
    for mut chunk in val_iter.by_ref() {
      let remain = Self::FIRST_CHUNK_SIZE - first_buf.len();
      if chunk.len() <= remain {
        first_buf.extend_from_slice(&chunk);
        if first_buf.len() >= Self::FIRST_CHUNK_SIZE {
          has_more = true;
          break;
        }
      } else {
        // Exceeds threshold, must use FILE mode / 超过阈值，必须用 FILE 模式
        first_buf.extend_from_slice(&chunk[..remain]);
        // Reuse chunk allocation: drain prefix, keep suffix / 复用 chunk 分配：移除前缀，保留后缀
        chunk.drain(..remain);
        leftover = Some(chunk);
        has_more = true;
        break;
      }
    }

    // Determine mode based on first chunk / 根据首块数据确定模式
    if !has_more && first_buf.len() <= INFILE_MAX {
      // Small enough for inline/infile, use normal put / 足够小，用普通 put
      let res = self.put(key, &first_buf).await;
      // Restore buffer / 归还 buffer
      self.stream_buf = first_buf;
      return res;
    }

    // Large value, must use FILE mode / 大值，必须用 FILE 模式
    // Stream value to bin file / 流式写入值到 bin 文件
    let val_id = self.ider.get();
    // Chain leftover with remaining iterator / 将剩余部分与迭代器链接
    let combined_iter = leftover.into_iter().chain(val_iter);
    let (val_len, val_crc) = self
      .write_file_stream_with_first(val_id, first_buf, combined_iter)
      .await?;

    // Inline key optimization: avoid put_with_file overhead for small keys
    // 内联键优化：小键避免 put_with_file 开销
    let k_mode = key_mode(key.len());
    if matches!(k_mode, Mode::Inline) {
      let head = Head::key_inline(key, Flag::FILE, Pos::new(val_id, 0), val_len, val_crc)?;
      self.write_head_only(head).await
    } else {
      self.put_with_file(key, val_id, val_len, val_crc).await
    }
  }

  /// Write head-only record (for inline key + file val) / 写入仅头记录
  async fn write_head_only(&mut self, head: Head) -> Result<Pos> {
    let total = (Head::SIZE + END_SIZE) as u64;
    self.reserve(total).await?;

    let start = self.cur_pos;
    let end = build_end(start);
    self.write_combined(&[head.as_bytes(), &end], start).await?;
    self.cur_pos += total;
    Ok(Pos::new(self.cur_id, start))
  }

  /// Write file in streaming mode with pre-read first chunk
  ///
  /// Write coalescing: buffer small chunks to reduce syscalls, improves throughput
  /// 写合并：缓冲小块减少系统调用，显著提高小分片流式写入吞吐量
  ///
  /// Returns (total_len, crc32) / 返回 (总长度, crc32)
  async fn write_file_stream_with_first<I>(
    &mut self,
    id: u64,
    first: Vec<u8>,
    iter: I,
  ) -> Result<(u32, u32)>
  where
    I: Iterator<Item = Vec<u8>>,
  {
    let path = self.bin_path(id);

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    let mut hasher = crc32fast::Hasher::new();

    // Write first chunk / 写入首块
    hasher.update(&first);
    let mut pos = first.len() as u64;
    let res = file.write_all_at(first, 0).await;
    res.0?;

    // Reclaim buffer for write coalescing / 取回缓冲区用于写合并
    let mut buf = res.1;
    let cap = buf.capacity();
    buf.clear();

    // Write remaining chunks with coalescing / 写入剩余块（带合并）
    for chunk in iter {
      hasher.update(&chunk);

      // Flush if adding chunk exceeds capacity / 若添加块超过容量则先刷盘
      if buf.len() + chunk.len() > cap && !buf.is_empty() {
        let len = buf.len();
        let res = file.write_all_at(buf, pos).await;
        res.0?;
        pos += len as u64;
        buf = res.1;
        buf.clear();
      }

      // Large chunk: write directly to avoid double copy / 大块：直接写入避免二次拷贝
      if chunk.len() > cap {
        let len = chunk.len();
        file.write_all_at(chunk, pos).await.0?;
        pos += len as u64;
      } else {
        buf.extend_from_slice(&chunk);
      }
    }

    // Flush remaining buffer / 刷入剩余缓冲区
    if !buf.is_empty() {
      let len = buf.len();
      let res = file.write_all_at(buf, pos).await;
      res.0?;
      pos += len as u64;
      buf = res.1;
    }

    // Restore buffer to WAL / 归还 buffer 给 WAL
    self.stream_buf = buf;

    Ok((pos as u32, hasher.finalize()))
  }

  /// Read key in streaming mode / 流式读取键
  pub async fn head_key_stream(&self, head: &Head) -> Result<DataStream> {
    if head.key_flag.is_inline() {
      return Ok(DataStream::Inline(head.key_data().to_vec()));
    }
    self
      .open_stream(head.key_flag, head.key_pos(), head.key_len.get() as u64)
      .await
  }

  /// Read value in streaming mode / 流式读取值
  pub async fn head_val_stream(&self, head: &Head) -> Result<DataStream> {
    if head.val_flag.is_inline() {
      return Ok(DataStream::Inline(head.val_data().to_vec()));
    }
    self
      .open_stream(head.val_flag, head.val_pos(), head.val_len.get() as u64)
      .await
  }

  /// Open data stream by flag and location / 根据标志和位置打开数据流
  async fn open_stream(&self, flag: Flag, loc: Pos, len: u64) -> Result<DataStream> {
    let path = if flag.is_file() {
      self.bin_path(loc.id())
    } else {
      self.wal_path(loc.id())
    };
    let file = OpenOptions::new().read(true).open(&path).await?;
    let pos = if flag.is_file() { 0 } else { loc.pos() };
    Ok(DataStream::File {
      file,
      pos,
      remain: len,
      buf: Vec::new(),
    })
  }
}

/// Data stream for reading large data / 用于读取大数据的流
pub enum DataStream {
  /// Inline data / 内联数据
  Inline(Vec<u8>),
  /// File-based stream / 基于文件的流
  File {
    file: File,
    pos: u64,
    remain: u64,
    buf: Vec<u8>,
  },
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
        // Safe: min ensures result fits in usize / 安全：min 确保结果适合 usize
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

  /// Read all remaining data / 读取所有剩余数据
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
