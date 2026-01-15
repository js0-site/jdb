use std::{borrow::Borrow, ptr};

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
  /// Decode and append to Vec, return bytes written.
  /// 解码并追加到 Vec，返回写入字节数。
  #[inline]
  pub fn decode(&self, data: &[u8], out: &mut Vec<u8>) -> usize {
    if data.is_empty() {
      return 0;
    }

    if self.n_symbols == 0 {
      out.extend_from_slice(data);
      return data.len();
    }

    let start = out.len();
    // Max expansion is 8x (each code -> 8 bytes) + safety margin
    // 最大扩展 8 倍 + 安全余量
    let needed = data.len() * 8 + 8;
    out.reserve(needed);

    let mut in_curr = 0;
    let mut out_curr = 0;
    let in_end = data.len();
    let symbols = &self.symbol;
    let lens = &self.len;

    unsafe {
      out.set_len(start + needed);
      let out_slice = &mut out[start..];

      // SIMD-style 4-byte batch processing
      // SIMD 风格 4 字节批量处理
      while in_curr + 4 <= in_end {
        let next_block = ptr::read_unaligned(data.as_ptr().add(in_curr) as *const u32);

        // Detect escape bytes (0xFF) using bit manipulation
        // 使用位运算检测转义字节
        let escape_mask = (next_block & 0x80808080u32)
          & ((((!next_block) & 0x7F7F7F7Fu32).wrapping_add(0x7F7F7F7Fu32)) ^ 0x80808080u32);

        if escape_mask == 0 {
          // No escape bytes, process 4 codes directly
          // 无转义字节，直接处理 4 个编码
          let code0 = *data.get_unchecked(in_curr) as usize;
          let src0 = *symbols.get_unchecked(code0);
          ptr::write_unaligned(out_slice.as_mut_ptr().add(out_curr) as *mut u64, src0);
          out_curr += *lens.get_unchecked(code0) as usize;
          in_curr += 1;

          let code1 = *data.get_unchecked(in_curr) as usize;
          let src1 = *symbols.get_unchecked(code1);
          ptr::write_unaligned(out_slice.as_mut_ptr().add(out_curr) as *mut u64, src1);
          out_curr += *lens.get_unchecked(code1) as usize;
          in_curr += 1;

          let code2 = *data.get_unchecked(in_curr) as usize;
          let src2 = *symbols.get_unchecked(code2);
          ptr::write_unaligned(out_slice.as_mut_ptr().add(out_curr) as *mut u64, src2);
          out_curr += *lens.get_unchecked(code2) as usize;
          in_curr += 1;

          let code3 = *data.get_unchecked(in_curr) as usize;
          let src3 = *symbols.get_unchecked(code3);
          ptr::write_unaligned(out_slice.as_mut_ptr().add(out_curr) as *mut u64, src3);
          out_curr += *lens.get_unchecked(code3) as usize;
          in_curr += 1;
        } else {
          // Has escape byte, find first escape position
          // 有转义字节，找到第一个转义位置
          let first_esc = escape_mask.trailing_zeros() >> 3;

          // Process codes before escape
          // 处理转义前的编码
          for _ in 0..first_esc {
            let code = *data.get_unchecked(in_curr) as usize;
            let src = *symbols.get_unchecked(code);
            ptr::write_unaligned(out_slice.as_mut_ptr().add(out_curr) as *mut u64, src);
            out_curr += *lens.get_unchecked(code) as usize;
            in_curr += 1;
          }

          // Handle escape: skip ESC byte, copy literal
          // 处理转义：跳过 ESC 字节，复制字面值
          in_curr += 1;
          if in_curr < in_end {
            *out_slice.get_unchecked_mut(out_curr) = *data.get_unchecked(in_curr);
            out_curr += 1;
            in_curr += 1;
          }
        }
      }

      // Handle remaining bytes
      // 处理剩余字节
      while in_curr < in_end {
        let code = *data.get_unchecked(in_curr) as usize;
        if code == ESC as usize {
          in_curr += 1;
          if in_curr < in_end {
            *out_slice.get_unchecked_mut(out_curr) = *data.get_unchecked(in_curr);
            out_curr += 1;
            in_curr += 1;
          }
        } else {
          let src = *symbols.get_unchecked(code);
          ptr::write_unaligned(out_slice.as_mut_ptr().add(out_curr) as *mut u64, src);
          out_curr += *lens.get_unchecked(code) as usize;
          in_curr += 1;
        }
      }

      out.set_len(start + out_curr);
    }

    out_curr
  }
}
