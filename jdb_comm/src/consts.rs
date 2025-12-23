//! 全局常量 Global constants

/// 页大小 4KB (Direct I/O)
pub const PAGE_SIZE: usize = 4096;

/// 页头大小
pub const PAGE_HEADER_SIZE: usize = 32;

/// 文件魔数 "JDB_FILE"
pub const FILE_MAGIC: u64 = 0x4A_44_42_5F_46_49_4C_45;

/// 无效页 ID
pub const INVALID_PAGE_ID: u32 = u32::MAX;

/// KV 分离阈值
pub const BLOB_THRESHOLD: usize = 512;
