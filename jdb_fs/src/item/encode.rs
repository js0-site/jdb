//! Encode trait for writing items
//! 用于写入条目的 Encode trait

use std::{future::Future, io};

use compio::io::{AsyncWrite, AsyncWriteExt};

use super::{CRC_SIZE, Item, MAGIC_SIZE};

/// Encode trait for writing items to async writer
/// 用于将条目写入异步写入器的 Encode trait
pub trait Encode: Item {
  /// Write length bytes to writer
  /// 将长度字节写入写入器
  fn write_len(
    len: usize,
    w: &mut (impl AsyncWrite + Unpin),
  ) -> impl Future<Output = io::Result<usize>>;

  /// Write length to buffer (for CRC calculation)
  /// 将长度写入缓冲区（用于 CRC 计算）
  fn write_len_to_buf(len: usize, out: &mut Vec<u8>);

  /// Write encoded data to writer, return bytes written
  /// 将编码数据写入写入器，返回写入的字节数
  ///
  /// Optimized: Single allocation, single write call, zero-copy CRC.
  /// 优化：单次分配，单次写入调用，零拷贝 CRC。
  fn write_data(
    data: &[u8],
    w: &mut (impl AsyncWrite + Unpin),
  ) -> impl Future<Output = io::Result<usize>> {
    async move {
      let data_len = data.len();
      let total_len = MAGIC_SIZE + Self::LEN_BYTES + data_len + CRC_SIZE;

      // Allocate one buffer for the whole packet
      // 为整个数据包分配一个缓冲区
      let mut buf = Vec::with_capacity(total_len);

      // 1. Header
      buf.push(Self::MAGIC);
      Self::write_len_to_buf(data_len, &mut buf);

      // 2. Data
      buf.extend_from_slice(data);

      // 3. CRC (Header + Data)
      let crc = crc32fast::hash(&buf);
      buf.extend_from_slice(&crc.to_le_bytes());

      // 4. Write all at once
      w.write_all(buf).await.0?;

      Ok(total_len)
    }
  }

  /// Encode data to Vec (convenience method / legacy)
  /// 将数据编码为 Vec（便捷方法/遗留）
  fn encode(data: &[u8]) -> Vec<u8> {
    let total = MAGIC_SIZE + Self::LEN_BYTES + data.len() + CRC_SIZE;
    let mut out = Vec::with_capacity(total);
    out.push(Self::MAGIC);
    Self::write_len_to_buf(data.len(), &mut out);
    out.extend_from_slice(data);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
  }
}
