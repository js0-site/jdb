//! Global constants 全局常量

/// Page size: 4KB (NVMe sector) 页大小：4KB（NVMe 扇区）
pub const PAGE_SIZE: usize = 4096;

/// Page header size 页头大小
pub const PAGE_HEADER_SIZE: usize = 32;

/// JDB file magic number JDB 文件魔数
pub const FILE_MAGIC: u64 = 0x4A_44_42_5F_46_49_4C_45;

/// Invalid PageID (null pointer) 无效 PageID（空指针）
pub const INVALID_PAGE_ID: u32 = u32::MAX;
