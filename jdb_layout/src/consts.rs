//! 常量定义 Constants

/// 页大小 Page size
pub const PAGE_SIZE: usize = 4096;

/// 页头大小 Page header size
pub const PAGE_HEADER_SIZE: usize = 32;

/// 无效页 ID Invalid page ID
pub const INVALID_PAGE_ID: u32 = u32::MAX;

/// 文件魔数 File magic
pub const FILE_MAGIC: u32 = 0x4A_44_42_50; // "JDBP"

/// BLOB 阈值 BLOB threshold
pub const BLOB_THRESHOLD: usize = 256;