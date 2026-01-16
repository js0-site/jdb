mod counter;
pub mod decode;
pub mod encode;
pub use decode::Decode;
pub use encode::Encode;
mod symbol;
mod table;

use std::io;

// === Constants ===
// === 常量 ===

// Escape byte indicating next byte is literal
// 转义字节，表示下一个字节是字面值
const ESC: u8 = 255;

// Max 512 symbols, 9 bits for code
// 最多 512 个符号，编码用 9 位
const CODE_BITS: u16 = 9;

// First 256 codes represent single bytes
// 前 256 个编码代表单字节
pub(crate) const CODE_BASE: u16 = 256;

// Max code value (512)
// 最大编码值
const CODE_MAX: u16 = 1 << CODE_BITS;

// Mask for all code bits
// 编码位掩码
const CODE_MASK: u16 = CODE_MAX - 1;

// Sample target size ~16KB
// 样本目标大小
const SAMPLETARGET: usize = 1 << 14;
const SAMPLEMAXSZ: usize = 2 * SAMPLETARGET;

/// Min input size to trigger encodeion.
/// 触发压缩的最小输入大小。
pub const LEAST_INPUT_SIZE: usize = 32 * 1024;

/// Min string length for FSST to be effective.
/// FSST 有效的最小字符串长度。
pub const LEAST_INPUT_MAX_LEN: u64 = 5;

// Free slot marker in hash table (bit 32 set)
// 哈希表中空槽标记
pub(crate) const ICL_FREE: u64 = 1 << 32;

// ICL field bit positions
// ICL 字段位位置
const CODE_LEN_SHIFT_IN_ICL: u64 = 28;
const CODE_SHIFT_IN_ICL: u64 = 16;
pub(crate) const CODE_LEN_SHIFT_IN_CODE: u64 = 12;

pub(crate) const HASH_TAB_SIZE: usize = 1024;
const MAX_SYMBOL_LEN: usize = 8;

/// Read u64 from unaligned pointer.
/// 从未对齐指针读取 u64。
///
/// # Safety
/// Caller must ensure `v` points to at least 8 bytes of readable memory.
/// 调用者必须确保 `v` 指向至少 8 字节的可读内存。
#[inline]
pub(crate) unsafe fn unaligned_load_unchecked(v: *const u8) -> u64 {
  unsafe { std::ptr::read_unaligned(v as *const u64) }
}

/// Train FSST encoder from list of items.
/// 从项目列表训练 FSST 编码器。
pub fn train<T: AsRef<[u8]>>(li: &[T]) -> io::Result<encode::Encode> {
  table::builder::build_from_items(li)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_symbol_new() {
    let st = table::Table::new();
    assert_eq!(st.n_symbols, 0);

    // Verify first 256 symbols are single-byte codes
    // 验证前 256 个符号是单字节编码
    for i in 0..=255u8 {
      assert_eq!(
        st.symbols[i as usize],
        symbol::Symbol::from_char(i, i as u16)
      );
    }

    // Verify hash table is empty
    // 验证哈希表为空
    assert!(st.hash_tab.iter().all(|s| *s == symbol::Symbol::new()));
  }
}
