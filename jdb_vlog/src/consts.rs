//! 常量定义 Constants

/// 头部大小 Header size
pub const HEADER: usize = 16; // len(4) + crc(4) + ts(8)

/// 页大小 Page size
pub const PAGE_SIZE: usize = 4096;