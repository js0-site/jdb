//! 常量定义 Constants

/// 页大小 Page size
pub const PAGE_SIZE: usize = 4096;

/// 页头大小 Page header size
pub const PAGE_HEADER_SIZE: usize = 32;

/// 无效页面 Invalid page
pub const INVALID_PAGE: u32 = u32::MAX;

// 状态位掩码 State bit masks
// [0..16]: Pin Count
// [16]: Usage Bit (CLOCK)
// [17]: Dirty Bit
// [18]: Valid Bit
pub const PIN_MASK: u64 = 0xFFFF;
pub const USAGE_BIT: u64 = 1 << 16;
pub const DIRTY_BIT: u64 = 1 << 17;
pub const VALID_BIT: u64 = 1 << 18;