//! Entry size calculation
//! 条目大小计算

/// Entry overhead: 4 (key_len) + 4 (val_len) + 1 (flag) + 4 (crc) = 13
/// 条目固定开销：4 (key_len) + 4 (val_len) + 1 (flag) + 4 (crc) = 13
pub const ENTRY_OVERHEAD: usize = 13;

/// Calculate entry size (key_len + val_len + overhead)
/// 计算条目大小（key_len + val_len + 固定开销）
#[inline]
pub const fn entry_size(key_len: usize, val_len: usize) -> usize {
  key_len + val_len + ENTRY_OVERHEAD
}
