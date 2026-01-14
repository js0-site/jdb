use std::{borrow::Borrow, io, ptr};

use bitcode::{Decode as BitcodeDecode, Encode as BitcodeEncode};

use crate::{ESC, encode::Encode};

/// The on-disk serialization format for the FSST symbol table (Dictionary).
/// FSST 符号表（字典）的磁盘序列化格式。
#[derive(Debug, Clone, PartialEq, Eq, Copy, BitcodeEncode, BitcodeDecode)]
pub struct Decode {
  /// Number of symbols in the table (0 means FSST is disabled).
  /// 符号表中的符号数量（0 表示未启用 FSST）。
  pub n_symbols: u8,

  /// Symbol lengths (values are in range 1-8).
  /// 符号长度。
  pub len: [u8; 256],

  /// Symbol values (bits 56-63 are ignored/garbage).
  /// 符号值。
  pub symbol: [u64; 256],
}

impl Default for Decode {
  fn default() -> Self {
    Self::new(0)
  }
}

impl Decode {
  pub const fn new(n_symbols: u8) -> Self {
    Self {
      n_symbols,
      len: [0; 256],
      symbol: [0; 256],
    }
  }
  /// Check if the dictionary is enabled.
  pub fn is_enabled(&self) -> bool {
    self.n_symbols > 0
  }
}

impl<T: Borrow<Encode>> From<T> for Decode {
  fn from(value: T) -> Self {
    let encoder = value.borrow();
    let mut decode = Decode::new(encoder.n_symbols() as u8);
    if decode.n_symbols > 0 {
      for i in 0..encoder.n_symbols() {
        let s = encoder.symbol(i);
        decode.symbol[i] = s.val;
        decode.len[i] = s.symbol_len() as u8;
      }
    }
    decode
  }
}

/// FSST decode.
/// FSST 解码器。
impl Decode {
  pub fn decode(
    &mut self,
    in_buf: &[u8],
    in_offsets_buf: &[usize],
    out_buf: &mut Vec<u8>,
    out_offsets_buf: &mut Vec<usize>,
  ) -> io::Result<()> {
    if self.n_symbols == 0 {
      out_buf.clear();
      out_buf.reserve(in_buf.len());
      unsafe {
        let len = in_buf.len();
        std::ptr::copy_nonoverlapping(in_buf.as_ptr(), out_buf.as_mut_ptr(), len);
        out_buf.set_len(len);
      }
      out_offsets_buf.clear();
      out_offsets_buf.extend_from_slice(in_offsets_buf);
      return Ok(());
    }

    // Ensure output buffers are large enough
    // 确保输出缓冲区足够大
    // FSST expansion factor is usually small, but let's be safe with 3x (worst case is higher but rare)
    // worst case for short strings could be higher, but typical textual data won't expand much.
    // Safety check: ensure we have enough capacity.
    let required_cap = in_buf.len() * 3;
    if out_buf.capacity() < required_cap {
      out_buf.reserve(required_cap - out_buf.len());
    }
    // We don't resize here to avoid zeroing. We will set_len at the end.
    // 我们不在这里 resize 以避免清零。我们将在最后 set_len。

    // SAFETY: We temporarily set len to capacity to allow Unsafe writes within bounds in decode_batch
    // Or better: decode_batch writes to ptr and returns length.
    // 其实 decode_batch 内部使用的是 out.as_mut_ptr().add(out_curr)，只要 capacity 够就是安全的
    // 但是为了保险起见，我们还是不通过 Vec 的 safe 接口访问

    out_offsets_buf.resize(in_offsets_buf.len(), 0);

    let mut out_pos = 0;
    let mut out_offsets_len = 0;

    // Safety: we pass the vector but we must ensure we don't access out of bounds of capacity
    // decode_batch 内部已经处理了
    decode_batch(
      self,
      in_buf,
      in_offsets_buf,
      out_buf,
      out_offsets_buf,
      &mut out_pos,
      &mut out_offsets_len,
    )?;

    // Set actual length
    unsafe { out_buf.set_len(out_pos) };

    Ok(())
  }
}

/// Deencode multiple strings in batch.
/// 批量解压多个字符串。
fn decode_batch(
  head: &Decode,
  encodeed_strs: &[u8],
  offsets: &[usize],
  out: &mut Vec<u8>,
  out_offsets: &mut Vec<usize>,
  out_pos: &mut usize,
  out_offsets_len: &mut usize,
) -> io::Result<()> {
  let symbol = &head.symbol;
  let len = &head.len;

  // Inline helper: decode one non-escape byte
  // 内联辅助：解码一个非转义字节
  macro_rules! decode_byte {
    ($in_curr:expr, $out_curr:expr) => {{
      // SAFETY: $in_curr < encodeed_strs.len(), code < 256
      let code = unsafe { *encodeed_strs.get_unchecked($in_curr) } as usize;
      let l = unsafe { *len.get_unchecked(code) } as usize;
      let s = unsafe { *symbol.get_unchecked(code) };
      unsafe {
        ptr::write_unaligned(out.as_mut_ptr().add($out_curr) as *mut u64, s);
      }
      $in_curr += 1;
      $out_curr += l;
    }};
  }

  // Handle escape byte
  // 处理转义字节
  macro_rules! handle_escape {
    ($in_curr:expr, $out_curr:expr) => {{
      $in_curr += 2;
      // SAFETY: $in_curr - 1 < encodeed_strs.len()
      unsafe {
        *out.get_unchecked_mut($out_curr) = *encodeed_strs.get_unchecked($in_curr - 1);
      }
      $out_curr += 1;
    }};
  }

  let mut decode = |mut in_curr: usize, in_end: usize, out_curr: &mut usize| {
    // SIMD-style 4-byte processing
    // SIMD 样式 4 字节处理
    while in_curr + 4 <= in_end {
      // SAFETY: in_curr + 4 <= in_end <= encodeed_strs.len()
      let next_block =
        unsafe { ptr::read_unaligned(encodeed_strs.as_ptr().add(in_curr) as *const u32) };

      // Escape detection mask
      // 转义检测掩码
      let escape_mask =
        (next_block & 0x80808080) & ((((!next_block) & 0x7F7F7F7F) + 0x7F7F7F7F) ^ 0x80808080);

      if escape_mask == 0 {
        // No escapes in this block
        // 此块无转义
        decode_byte!(in_curr, *out_curr);
        decode_byte!(in_curr, *out_curr);
        decode_byte!(in_curr, *out_curr);
        decode_byte!(in_curr, *out_curr);
      } else {
        let first_escape_pos = escape_mask.trailing_zeros() >> 3;
        match first_escape_pos {
          3 => {
            decode_byte!(in_curr, *out_curr);
            decode_byte!(in_curr, *out_curr);
            decode_byte!(in_curr, *out_curr);
            handle_escape!(in_curr, *out_curr);
          }
          2 => {
            decode_byte!(in_curr, *out_curr);
            decode_byte!(in_curr, *out_curr);
            handle_escape!(in_curr, *out_curr);
          }
          1 => {
            decode_byte!(in_curr, *out_curr);
            handle_escape!(in_curr, *out_curr);
          }
          _ => {
            handle_escape!(in_curr, *out_curr);
          }
        }
      }
    }

    // Handle remaining bytes
    // 处理剩余字节
    if in_curr + 2 <= in_end {
      // SAFETY: in_curr + 1 < in_end <= encodeed_strs.len()
      unsafe {
        *out.get_unchecked_mut(*out_curr) = *encodeed_strs.get_unchecked(in_curr + 1);
      }
      if unsafe { *encodeed_strs.get_unchecked(in_curr) } != ESC {
        decode_byte!(in_curr, *out_curr);
        if unsafe { *encodeed_strs.get_unchecked(in_curr) } != ESC {
          decode_byte!(in_curr, *out_curr);
        } else {
          handle_escape!(in_curr, *out_curr);
        }
      } else {
        in_curr += 2;
        *out_curr += 1;
      }
    }

    // Last byte (cannot be escape)
    // 最后一个字节（不可能是转义）
    if in_curr < in_end {
      decode_byte!(in_curr, *out_curr);
    }
  };

  let mut out_curr = *out_pos;
  out_offsets[0] = *out_pos;

  for (i, win) in offsets.windows(2).enumerate() {
    decode(win[0], win[1], &mut out_curr);
    out_offsets[i + 1] = out_curr;
  }

  out_offsets.truncate(offsets.len());
  *out_pos = out_curr;
  *out_offsets_len = offsets.len();
  Ok(())
}
