//! Stream copy utilities / 流对拷工具
//!
//! Copy data between slots using streaming.
//! 使用流式方式在槽位间拷贝数据。

use crate::{Result, SlabClass, SlabReader, SlabWriter, SlotId};

/// Default buffer size for streaming / 流式传输默认缓冲区大小
pub const DEFAULT_BUF_SIZE: usize = 16384;

/// Copy data between slots using streaming / 使用流式方式在槽位间拷贝数据
pub async fn stream_copy(
  src_slab: &SlabClass,
  src_slot: SlotId,
  src_len: u64,
  dst_slab: &mut SlabClass,
) -> Result<SlotId> {
  let mut reader = src_slab.reader(src_slot, src_len);
  let mut writer = dst_slab.writer().await?;
  pipe(&mut reader, &mut writer).await?;
  writer.finish().await
}

/// Copy with transformation (e.g., compression) / 带转换的拷贝（如压缩）
pub async fn stream_copy_with<F>(
  src_slab: &SlabClass,
  src_slot: SlotId,
  src_len: u64,
  dst_slab: &mut SlabClass,
  mut transform: F,
) -> Result<SlotId>
where
  F: FnMut(&[u8]) -> Vec<u8>,
{
  let mut reader = src_slab.reader(src_slot, src_len);
  let mut writer = dst_slab.writer().await?;

  while !reader.is_done() {
    let chunk = reader.read(DEFAULT_BUF_SIZE).await?;
    if chunk.is_empty() {
      break;
    }
    let transformed = transform(&chunk);
    writer.write(&transformed)?;
  }

  writer.finish().await
}

/// Pipe reader to writer directly / 直接管道连接读写器
pub async fn pipe(reader: &mut SlabReader<'_>, writer: &mut SlabWriter<'_>) -> Result<u64> {
  pipe_with(reader, writer, DEFAULT_BUF_SIZE).await
}

/// Pipe reader to writer with custom buffer size / 使用自定义缓冲区大小管道连接
pub async fn pipe_with(
  reader: &mut SlabReader<'_>,
  writer: &mut SlabWriter<'_>,
  buf_size: usize,
) -> Result<u64> {
  let mut total = 0u64;

  while !reader.is_done() {
    let chunk = reader.read(buf_size).await?;
    if chunk.is_empty() {
      break;
    }
    let n = writer.write(&chunk)?;
    total += n as u64;
  }

  Ok(total)
}
