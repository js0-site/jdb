//! Encode functions for writing items
//! 用于写入条目的编码函数

use compio::io::{AsyncWrite, AsyncWriteExt};
use zbin::Bin;
use zerocopy::IntoBytes;

use super::{Error, Item, Result, Row};

/// Encode head to Row
/// 编码 head 为 Row
#[inline]
pub fn encode<I: Item>(head: I::Head) -> Row<I::Head> {
  let crc32 = crc32fast::hash(head.as_bytes());
  Row {
    magic: I::MAGIC,
    head,
    crc32,
  }
}

/// Write head and data to writer, return bytes written
/// 将 head 和 data 写入写入器，返回写入的字节数
#[inline]
pub async fn write<'a, I: Item>(
  head: I::Head,
  data: impl Bin<'a>,
  w: &mut (impl AsyncWrite + Unpin),
) -> Result<u64> {
  let row = encode::<I>(head);
  let row_bytes = row.as_bytes();
  let row_len = row_bytes.len();
  // Use Box for owned buffer required by compio
  // 使用 Box 作为 compio 需要的 owned buffer
  w.write_all(Box::<[u8]>::from(row_bytes))
    .await
    .0
    .map_err(Error::Io)?;

  let data_len = data.len();
  if data_len > 0 {
    w.write_all(data.io()).await.0.map_err(Error::Io)?;
  }

  Ok((row_len + data_len) as u64)
}
