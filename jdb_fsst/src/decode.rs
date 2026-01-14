use std::{io, ptr};

use crate::{ESC, dict::Dict};

/// FSST decode.
/// FSST 解码器。
impl Dict {
  pub fn decode(
    &mut self,
    in_buf: &[u8],
    in_offsets_buf: &[usize],
    out_buf: &mut Vec<u8>,
    out_offsets_buf: &mut Vec<usize>,
  ) -> io::Result<()> {
    if self.n_symbols == 0 {
      out_buf.resize(in_buf.len(), 0);
      out_buf.copy_from_slice(in_buf);
      out_offsets_buf.resize(in_offsets_buf.len(), 0);
      out_offsets_buf.copy_from_slice(in_offsets_buf);
      return Ok(());
    }

    // Ensure output buffers are large enough
    if out_buf.len() < in_buf.len() * 3 {
      out_buf.resize(in_buf.len() * 3, 0);
    }
    if out_offsets_buf.len() < in_offsets_buf.len() {
      out_offsets_buf.resize(in_offsets_buf.len(), 0);
    }

    let mut out_pos = 0;
    let mut out_offsets_len = 0;
    decode_batch(
      self,
      in_buf,
      in_offsets_buf,
      out_buf,
      out_offsets_buf,
      &mut out_pos,
      &mut out_offsets_len,
    )?;
    Ok(())
  }
}

/// Deencode multiple strings in batch.
/// 批量解压多个字符串。
fn decode_batch(
  head: &Dict,
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

  out.truncate(out_curr);
  out_offsets.truncate(offsets.len());
  *out_pos = out_curr;
  *out_offsets_len = offsets.len();
  Ok(())
}
