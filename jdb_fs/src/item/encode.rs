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

  /// Write encoded data to writer, return bytes written
  /// 将编码数据写入写入器，返回写入的字节数
  fn write_data(
    data: &[u8],
    w: &mut (impl AsyncWrite + Unpin),
  ) -> impl Future<Output = io::Result<usize>> {
    async {
      // Build complete buffer to avoid lifetime issues with compio
      // 构建完整缓冲区以避免 compio 的生命周期问题
      let encoded = Self::encode(data);
      let len = encoded.len();
      w.write_all(encoded).await.0?;
      Ok(len)
    }
  }

  /// Write length to buffer (for CRC calculation)
  /// 将长度写入缓冲区（用于 CRC 计算）
  fn write_len_to_buf(len: usize, out: &mut Vec<u8>);

  /// Encode data to Vec (convenience method)
  /// 将数据编码为 Vec（便捷方法）
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
