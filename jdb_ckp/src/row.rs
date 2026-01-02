//! Checkpoint row format
//! 检查点行格式
//!
//! Disk format: magic(1) + kind(1) + wal_id(8) + [offset(8)] + crc32(4)
//! 磁盘格式：magic(1) + kind(1) + wal_id(8) + [offset(8)] + crc32(4)

use jdb_base::{WalId, WalOffset};

/// Magic byte
/// 魔数
pub const MAGIC: u8 = 0x42;

pub(crate) const KIND_SAVE: u8 = 1;
pub(crate) const KIND_ROTATE: u8 = 2;
pub(crate) const CRC_SIZE: usize = 4;
const WAL_ID_SIZE: usize = 8;
const OFFSET_SIZE: usize = 8;
pub(crate) const MAGIC_SIZE: usize = 1;
const KIND_SIZE: usize = 1;
pub(crate) const HEAD_SIZE: usize = MAGIC_SIZE + KIND_SIZE;

/// Rotate entry size: magic(1) + kind(1) + wal_id(8) + crc32(4) = 14
pub const ROTATE_SIZE: usize = HEAD_SIZE + WAL_ID_SIZE + CRC_SIZE;

/// Save entry size: magic(1) + kind(1) + wal_id(8) + offset(8) + crc32(4) = 22
pub const SAVE_SIZE: usize = ROTATE_SIZE + OFFSET_SIZE;

/// Meta size for rotate: kind(1) + wal_id(8) = 9
pub(crate) const ROTATE_META: usize = KIND_SIZE + WAL_ID_SIZE;

/// Meta size for save: kind(1) + wal_id(8) + offset(8) = 17
pub(crate) const SAVE_META: usize = ROTATE_META + OFFSET_SIZE;

/// Parsed head
/// 解析的头
#[derive(Debug, Clone, Copy)]
pub enum Head {
  Save { wal_id: WalId, offset: WalOffset },
  Rotate { wal_id: WalId },
}

/// Bytes for save entry
/// 保存条目的字节
pub type SaveBytes = [u8; SAVE_SIZE];

/// Bytes for rotate entry
/// 轮转条目的字节
pub type RotateBytes = [u8; ROTATE_SIZE];

/// Row for disk write
/// 磁盘写入的行
#[derive(Debug, Clone, Copy)]
pub enum Row {
  Save { wal_id: WalId, offset: WalOffset },
  Rotate { wal_id: WalId },
}

impl Row {
  /// Convert to bytes (stack array)
  /// 转换为字节（栈数组）
  #[inline]
  pub fn save_bytes(wal_id: WalId, offset: WalOffset) -> SaveBytes {
    let mut buf = [0u8; SAVE_SIZE];
    buf[0] = MAGIC;
    buf[1] = KIND_SAVE;
    buf[HEAD_SIZE..HEAD_SIZE + WAL_ID_SIZE].copy_from_slice(&wal_id.to_le_bytes());
    buf[HEAD_SIZE + WAL_ID_SIZE..HEAD_SIZE + WAL_ID_SIZE + OFFSET_SIZE]
      .copy_from_slice(&offset.to_le_bytes());
    let crc = crc32fast::hash(&buf[MAGIC_SIZE..SAVE_SIZE - CRC_SIZE]);
    buf[SAVE_SIZE - CRC_SIZE..].copy_from_slice(&crc.to_le_bytes());
    buf
  }

  /// Convert to bytes (stack array)
  /// 转换为字节（栈数组）
  #[inline]
  pub fn rotate_bytes(wal_id: WalId) -> RotateBytes {
    let mut buf = [0u8; ROTATE_SIZE];
    buf[0] = MAGIC;
    buf[1] = KIND_ROTATE;
    buf[HEAD_SIZE..HEAD_SIZE + WAL_ID_SIZE].copy_from_slice(&wal_id.to_le_bytes());
    let crc = crc32fast::hash(&buf[MAGIC_SIZE..ROTATE_SIZE - CRC_SIZE]);
    buf[ROTATE_SIZE - CRC_SIZE..].copy_from_slice(&crc.to_le_bytes());
    buf
  }

  /// Convert to Vec bytes (for async IO)
  /// 转换为 Vec 字节（用于异步 IO）
  #[inline]
  pub fn to_vec(self) -> Vec<u8> {
    match self {
      Row::Save { wal_id, offset } => Self::save_bytes(wal_id, offset).to_vec(),
      Row::Rotate { wal_id } => Self::rotate_bytes(wal_id).to_vec(),
    }
  }
}
