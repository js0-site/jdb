//! WAL entry protocol WAL 条目协议
//! Binary serialization via bitcode 通过 bitcode 二进制序列化

use bitcode::{Decode, Encode};

// Type aliases for common types
pub type Lsn = u64;         // Log Sequence Number
pub type TableID = u32;     // Table identifier  
pub type Timestamp = u64;   // Timestamp in microseconds

/// WAL entry type WAL 条目类型
#[derive(Debug, Clone, Encode, Decode)]
pub enum WalEntry {
  /// Put key-value 写入键值
  Put {
    table: TableID,
    ts: Timestamp,
    key: Vec<u8>,
    val: Vec<u8>,
  },
  /// Delete key 删除键
  Delete {
    table: TableID,
    ts: Timestamp,
    key: Vec<u8>,
  },
  /// Barrier for group commit 组提交屏障
  Barrier { lsn: Lsn },
}

/// Encode WAL entry 编码 WAL 条目
#[inline]
pub fn encode(entry: &WalEntry) -> Vec<u8> {
  bitcode::encode(entry)
}

/// Decode WAL entry 解码 WAL 条目
#[inline]
pub fn decode(data: &[u8]) -> Result<WalEntry, bitcode::Error> {
  bitcode::decode(data)
}