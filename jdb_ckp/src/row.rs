//! Checkpoint row format
//! 检查点行格式
//!
//! Disk format: magic(1) + kind(1) + wal_id(8) + [offset(8)] + crc32(4)
//! 磁盘格式：magic(1) + kind(1) + wal_id(8) + [offset(8)] + crc32(4)

use jdb_base::{WalId, WalOffset};
use zerocopy::little_endian::U64;

/// Magic byte
/// 魔数
pub const MAGIC: u8 = 0x42;

pub(crate) const KIND_SAVE: u8 = 1;
pub(crate) const KIND_ROTATE: u8 = 2;
pub(crate) const MAGIC_SIZE: usize = 1;

/// Rotate entry size: magic(1) + kind(1) + wal_id(8) + crc32(4) = 14
/// 轮转条目大小
pub const ROTATE_SIZE: usize = 14;

/// Save entry size: magic(1) + kind(1) + wal_id(8) + offset(8) + crc32(4) = 22
/// 保存条目大小
pub const SAVE_SIZE: usize = ROTATE_SIZE + size_of::<U64>();

/// Row for disk read
/// 磁盘读取的行
#[derive(Debug, Clone, Copy)]
pub enum Row {
  SaveWalPtr { wal_id: WalId, offset: WalOffset },
  Rotate { wal_id: WalId },
}
