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
  /// Optimized to avoid intermediate allocation by writing parts directly.
  /// 优化：通过分段直接写入避免中间分配。
  fn write_data(
    data: &[u8],
    w: &mut (impl AsyncWrite + Unpin),
  ) -> impl Future<Output = io::Result<usize>> {
    async move {
      let mut written = 0;

      // 1. Write Header (Magic + Len)
      // 1. 写入头部 (魔数 + 长度)
      let mut head = Vec::with_capacity(MAGIC_SIZE + Self::LEN_BYTES);
      head.push(Self::MAGIC);
      Self::write_len_to_buf(data.len(), &mut head);
      w.write_all(head.clone()).await.0?;
      written += MAGIC_SIZE + Self::LEN_BYTES;

      // 2. Write Data
      // 2. 写入数据
      w.write_all(data.to_vec()).await.0?;
      written += data.len();

      // 3. Calculate and Write CRC
      // 3. 计算并写入 CRC
      let mut crc_hasher = crc32fast::Hasher::new();
      crc_hasher.update(&head);
      crc_hasher.update(data);
      let crc = crc_hasher.finalize();
      w.write_all(crc.to_le_bytes().to_vec()).await.0?;
      written += CRC_SIZE;

      Ok(written)
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
