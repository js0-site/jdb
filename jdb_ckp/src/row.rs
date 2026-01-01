use bitcode::{Decode, Encode};
use compio::io::AsyncReadAt;
use crc32fast::hash;

use crate::ckp::{WalId, WalOffset};

/// Magic byte for DiskRow validation
const MAGIC: u8 = 0x42;

/// Row types for checkpoint entries
/// 检查点条目的行类型
#[derive(Encode, Decode, Debug, Clone)]
pub enum Row {
  Save { wal_id: WalId, offset: WalOffset },
  Rotate { wal_id: WalId },
}

/// DiskRow represents the actual format written to disk
/// DiskRow 表示磁盘上的实际格式
#[derive(Debug, Clone)]
pub struct DiskRow {
  /// Magic byte (1 byte) for validation
  pub magic: u8,
  /// CRC32 checksum (4 bytes) of data only
  pub crc32: u32,
  /// Length prefix (1 byte)
  pub len: u8,
  /// Encoded Row data
  pub data: Vec<u8>,
}

impl DiskRow {
  /// Get total size in bytes
  pub fn total_size(&self) -> u64 {
    1 + 4 + 1 + self.data.len() as u64
  }

  /// Convert to bytes for writing to disk
  pub fn to_bytes(&self) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + 4 + 1 + self.data.len());
    bytes.push(self.magic);
    bytes.extend_from_slice(&self.crc32.to_be_bytes());
    bytes.push(self.len);
    bytes.extend_from_slice(&self.data);
    bytes
  }

  /// Parse from bytes read from disk
  /// 从磁盘读取的字节解析
  pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::error::Error> {
    // Minimum size: magic(1) + crc32(4) + len(1) = 6 bytes
    if bytes.len() < 6 {
      return Err(crate::error::Error::Corrupted(0));
    }

    // Read magic
    let magic = bytes[0];
    if magic != MAGIC {
      return Err(crate::error::Error::Corrupted(0));
    }

    // Read crc32
    let crc32 = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);

    // Read len
    let len = bytes[5];
    let total_len = 1 + 4 + 1 + len as usize;

    if bytes.len() < total_len {
      return Err(crate::error::Error::Corrupted(0));
    }

    // Extract data
    let data = bytes[6..total_len].to_vec();

    // Verify crc32 (covers data only)
    let computed_crc32 = hash(&data);

    if computed_crc32 != crc32 {
      return Err(crate::error::Error::Corrupted(0));
    }

    Ok(Self {
      magic,
      crc32,
      len,
      data,
    })
  }

  /// Read DiskRow from file at given position
  /// 从文件指定位置读取 DiskRow
  pub async fn from_file(file: &compio::fs::File, pos: u64) -> Result<Self, crate::error::Error> {
    // Read magic (1 byte)
    let magic_buf: Box<[u8]> = vec![0u8; 1].into_boxed_slice();
    let res = file.read_at(magic_buf, pos).await;
    res.0?;
    let magic = res.1[0];

    // Read crc32 (4 bytes)
    let crc_buf: Box<[u8]> = vec![0u8; 4].into_boxed_slice();
    let res = file.read_at(crc_buf, pos + 1).await;
    res.0?;
    let crc32 = u32::from_be_bytes([res.1[0], res.1[1], res.1[2], res.1[3]]);

    // Read len (1 byte)
    let len_buf: Box<[u8]> = vec![0u8; 1].into_boxed_slice();
    let res = file.read_at(len_buf, pos + 5).await;
    res.0?;
    let len = res.1[0];

    // Read body
    let body_buf: Box<[u8]> = vec![0u8; len as usize].into_boxed_slice();
    let res = file.read_at(body_buf, pos + 6).await;
    res.0?;
    let data = res.1.into_vec();

    // Verify magic
    if magic != MAGIC {
      return Err(crate::error::Error::Corrupted(0));
    }

    // Verify crc32 (covers data only)
    let computed_crc32 = hash(&data);

    if computed_crc32 != crc32 {
      return Err(crate::error::Error::Corrupted(0));
    }

    Ok(Self {
      magic,
      crc32,
      len,
      data,
    })
  }

  /// Try to read DiskRow from file at given position with file length check
  /// 尝试从文件读取 DiskRow，带有文件长度检查
  /// Returns None if data is incomplete
  /// 如果数据不完整返回 None
  pub async fn try_from_file(
    file: &compio::fs::File,
    pos: u64,
    file_len: u64,
  ) -> Result<Option<Self>, crate::error::Error> {
    // Check if we have enough bytes for magic (1 byte)
    if file_len - pos < 1 {
      return Ok(None);
    }

    // Read magic (1 byte)
    let magic_buf: Box<[u8]> = vec![0u8; 1].into_boxed_slice();
    let res = file.read_at(magic_buf, pos).await;
    res.0?;
    let magic = res.1[0];

    // Check if we have enough bytes for crc32 (4 bytes)
    if file_len - pos - 1 < 4 {
      return Ok(None);
    }

    // Read crc32 (4 bytes)
    let crc_buf: Box<[u8]> = vec![0u8; 4].into_boxed_slice();
    let res = file.read_at(crc_buf, pos + 1).await;
    res.0?;
    let crc32 = u32::from_be_bytes([res.1[0], res.1[1], res.1[2], res.1[3]]);

    // Check if we have enough bytes for len (1 byte)
    if file_len - pos - 5 < 1 {
      return Ok(None);
    }

    // Read len (1 byte)
    let len_buf: Box<[u8]> = vec![0u8; 1].into_boxed_slice();
    let res = file.read_at(len_buf, pos + 5).await;
    res.0?;
    let len = res.1[0];

    // Check if we have enough bytes for the body
    if file_len - pos - 6 < len as u64 {
      return Ok(None);
    }

    // Read body
    let body_buf: Box<[u8]> = vec![0u8; len as usize].into_boxed_slice();
    let res = file.read_at(body_buf, pos + 6).await;
    res.0?;
    let data = res.1.into_vec();

    // Verify magic
    if magic != MAGIC {
      return Err(crate::error::Error::Corrupted(0));
    }

    // Verify crc32 (covers data only)
    let computed_crc32 = hash(&data);

    if computed_crc32 != crc32 {
      return Err(crate::error::Error::Corrupted(0));
    }

    Ok(Some(Self {
      magic,
      crc32,
      len,
      data,
    }))
  }
}

impl From<Row> for DiskRow {
  fn from(row: Row) -> Self {
    let data = bitcode::encode(&row);
    debug_assert!(data.len() <= u8::MAX as usize, "Row data too large");
    let len = data.len() as u8;
    let magic = MAGIC;

    // Compute crc32 over data only
    let crc32 = hash(&data);

    Self {
      magic,
      crc32,
      len,
      data,
    }
  }
}

impl TryInto<Row> for DiskRow {
  type Error = crate::error::Error;

  fn try_into(self) -> Result<Row, Self::Error> {
    bitcode::decode(&self.data).map_err(Into::into)
  }
}
