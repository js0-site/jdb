//! Checkpoint row format
//! 检查点行格式
//!
//! Disk format: magic(1) + kind(1) + data + crc32(4)
//! 磁盘格式：magic(1) + kind(1) + data + crc32(4)

use jdb_base::{WalId, WalOffset};

pub(crate) const KIND_SAVE: u8 = 1;
pub(crate) const KIND_ROTATE: u8 = 2;
pub(crate) const KIND_SST_ADD: u8 = 3;
pub(crate) const KIND_SST_RM: u8 = 4;

/// Row for disk read
/// 磁盘读取的行
#[derive(Debug, Clone, Copy)]
pub enum Row {
  SaveWalPtr { wal_id: WalId, offset: WalOffset },
  Rotate { wal_id: WalId },
  SstAdd { id: u64, level: u8 },
  SstRm { id: u64 },
}

/// Batch operation for atomic writes
/// 原子写入的批量操作
#[derive(Debug, Clone, Copy)]
pub enum Op {
  /// Add SSTable to level / 添加 SSTable 到层级
  SstAdd(u64, u8),
  /// Remove SSTable / 删除 SSTable
  SstRm(u64),
}
