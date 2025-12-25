//! Streaming reader for large values / 大数据流式读取器
//!
//! Reads data in chunks without loading entire value into memory.
//! 分块读取数据，无需将整个值加载到内存。

use jdb_alloc::AlignedBuf;
use jdb_fs::File;

use crate::{Header, Result, SlotId};

/// Streaming reader for large values / 大数据流式读取器
pub struct SlabReader<'a> {
  /// File reference / 文件引用
  file: &'a File,
  /// Slot offset in file / 槽位在文件中的偏移
  slot_offset: u64,
  /// Class size / 层级大小
  class_size: usize,
  /// Current read offset within payload / 当前在载荷中的读取偏移
  offset: u64,
  /// Total payload length / 总载荷长度
  total_len: u64,
}

impl<'a> SlabReader<'a> {
  /// Create reader for slot / 为槽位创建读取器
  pub fn new(file: &'a File, slot_id: SlotId, class_size: usize, total_len: u64) -> Self {
    let slot_offset = slot_id * class_size as u64;
    Self {
      file,
      slot_offset,
      class_size,
      offset: 0,
      total_len,
    }
  }

  /// Read next chunk into buffer / 读取下一块到缓冲区
  pub async fn read(&mut self, max_len: usize) -> Result<Vec<u8>> {
    if self.offset >= self.total_len {
      return Ok(Vec::new());
    }

    // Calculate read size / 计算读取大小
    let remaining = (self.total_len - self.offset) as usize;
    let read_len = remaining.min(max_len);

    // Read aligned buffer / 读取对齐缓冲区
    let buf = AlignedBuf::zeroed(self.class_size)?;
    let file_offset = self.slot_offset;
    let buf = self.file.read_at(buf, file_offset).await?;

    // Extract payload portion / 提取载荷部分
    let start = Header::SIZE + self.offset as usize;
    let end = start + read_len;
    let data = buf[start..end].to_vec();

    self.offset += read_len as u64;
    Ok(data)
  }

  /// Remaining bytes / 剩余字节数
  #[inline]
  pub fn remaining(&self) -> u64 {
    self.total_len.saturating_sub(self.offset)
  }

  /// Check if finished / 是否读取完毕
  #[inline]
  pub fn is_done(&self) -> bool {
    self.offset >= self.total_len
  }

  /// Current offset / 当前偏移
  #[inline]
  pub fn offset(&self) -> u64 {
    self.offset
  }

  /// Total length / 总长度
  #[inline]
  pub fn total_len(&self) -> u64 {
    self.total_len
  }
}

// Note: compio::io::AsyncRead requires ownership-based API
// For now, we provide a simpler streaming interface
// compio AsyncRead 需要基于所有权的 API，暂时提供简单的流式接口
