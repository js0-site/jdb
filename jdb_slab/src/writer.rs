//! Streaming writer for large values / 大数据流式写入器
//!
//! Writes data in chunks without buffering everything.
//! 分块写入数据，无需缓冲所有内容。

use crc32fast::Hasher;
use jdb_alloc::AlignedBuf;
use jdb_fs::File;

use crate::{Compress, Error, Header, Result, SlotId};

/// Streaming writer for large values / 大数据流式写入器
pub struct SlabWriter<'a> {
  /// File reference / 文件引用
  file: &'a File,
  /// Slot id / 槽位 ID
  slot_id: SlotId,
  /// Slot offset in file / 槽位在文件中的偏移
  slot_offset: u64,
  /// Current write offset within payload / 当前在载荷中的写入偏移
  offset: u64,
  /// Max payload length / 最大载荷长度
  max_len: u64,
  /// CRC32 hasher / CRC32 哈希器
  hasher: Hasher,
  /// Compression type / 压缩类型
  compress: Compress,
  /// Buffer for accumulating data / 累积数据的缓冲区
  buf: AlignedBuf,
}

impl<'a> SlabWriter<'a> {
  /// Create writer for new slot / 为新槽位创建写入器
  pub fn new(
    file: &'a File,
    slot_id: SlotId,
    class_size: usize,
    compress: Compress,
  ) -> Result<Self> {
    let slot_offset = slot_id * class_size as u64;
    let max_len = (class_size - Header::SIZE) as u64;
    let buf = AlignedBuf::zeroed(class_size)?;

    Ok(Self {
      file,
      slot_id,
      slot_offset,
      offset: 0,
      max_len,
      hasher: Hasher::new(),
      compress,
      buf,
    })
  }

  /// Write chunk from buffer / 从缓冲区写入块
  pub fn write(&mut self, data: &[u8]) -> Result<usize> {
    let remaining = (self.max_len - self.offset) as usize;
    if remaining == 0 {
      return Err(Error::Overflow {
        len: data.len(),
        max: 0,
      });
    }

    let write_len = data.len().min(remaining);
    let start = Header::SIZE + self.offset as usize;
    self.buf[start..start + write_len].copy_from_slice(&data[..write_len]);
    self.hasher.update(&data[..write_len]);
    self.offset += write_len as u64;

    Ok(write_len)
  }

  /// Finalize write, compute CRC and update header / 完成写入，计算 CRC 并更新头部
  pub async fn finish(mut self) -> Result<SlotId> {
    // Compute CRC32 / 计算 CRC32
    let crc32 = self.hasher.finalize();

    // Build header / 构建头部
    let header = Header::new(crc32, self.offset as u32, self.compress);
    self.buf[..Header::SIZE].copy_from_slice(&header.encode());

    // Write to file / 写入文件
    self.file.write_at(self.buf, self.slot_offset).await?;

    Ok(self.slot_id)
  }

  /// Remaining capacity / 剩余容量
  #[inline]
  pub fn remaining(&self) -> u64 {
    self.max_len.saturating_sub(self.offset)
  }

  /// Current written length / 当前已写入长度
  #[inline]
  pub fn written(&self) -> u64 {
    self.offset
  }

  /// Slot id / 槽位 ID
  #[inline]
  pub fn slot_id(&self) -> SlotId {
    self.slot_id
  }
}

// Note: compio::io::AsyncWrite requires ownership-based API
// For now, we provide a simpler streaming interface
// compio AsyncWrite 需要基于所有权的 API，暂时提供简单的流式接口
