use std::{borrow::Borrow, ptr};

use bitcode::{Decode as BitcodeDecode, Encode as BitcodeEncode};

use crate::ESC;

// Check if u64 contains ESC (0xFF) byte using SWAR
// 使用 SWAR 检查 u64 是否包含 ESC (0xFF) 字节
#[inline(always)]
const fn has_esc(block: u64) -> bool {
  let t = block ^ 0xFFFFFFFFFFFFFFFF;
  (t.wrapping_sub(0x0101010101010101)) & !t & 0x8080808080808080 != 0
}

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

impl<T: Borrow<crate::encode::Encode>> From<T> for Decode {
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
  /// Decode single code, return output length.
  /// 解码单个编码，返回输出长度。
  #[inline(always)]
  unsafe fn decode_one(&self, code: usize, out_ptr: *mut u8) -> usize {
    unsafe {
      let src = *self.symbol.get_unchecked(code);
      ptr::write_unaligned(out_ptr as *mut u64, src);
      *self.len.get_unchecked(code) as usize
    }
  }

  /// Decode 8 codes from a block without ESC, return bytes written.
  /// 从无 ESC 的块解码 8 个编码，返回写入字节数。
  #[inline(always)]
  unsafe fn decode_block(&self, block: u64, out_ptr: *mut u8) -> usize {
    unsafe {
      let sym = &self.symbol;
      let lens = &self.len;
      let mut off = 0usize;

      macro_rules! dec {
        ($sh:expr) => {{
          let c = ((block >> $sh) & 0xFF) as usize;
          ptr::write_unaligned(out_ptr.add(off) as *mut u64, *sym.get_unchecked(c));
          off += *lens.get_unchecked(c) as usize;
        }};
      }

      dec!(0);
      dec!(8);
      dec!(16);
      dec!(24);
      dec!(32);
      dec!(40);
      dec!(48);
      dec!(56);
      off
    }
  }

  /// Decode to Box<[u8]>.
  /// 解码到 Box<[u8]>。
  #[inline]
  pub fn decode_boxed(&self, data: &[u8]) -> Box<[u8]> {
    // Pre-allocate with estimated capacity (avg expansion ~3x)
    // 预分配估算容量（平均扩展约 3 倍）
    let mut out = Vec::with_capacity(data.len() * 3);
    self.decode(data, &mut out);
    out.into_boxed_slice()
  }

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

    let in_end = data.len();
    let in_ptr = data.as_ptr();

    unsafe {
      out.set_len(start + needed);
      let out_base = out.as_mut_ptr().add(start);
      let mut in_curr = 0;
      let mut out_curr = 0;

      // Main loop: 8-byte batch with two blocks
      // 主循环：双 8 字节块批处理
      while in_curr + 16 <= in_end {
        let block0 = ptr::read_unaligned(in_ptr.add(in_curr) as *const u64);
        let block1 = ptr::read_unaligned(in_ptr.add(in_curr + 8) as *const u64);

        let has_esc0 = has_esc(block0);
        let has_esc1 = has_esc(block1);

        if !has_esc0 && !has_esc1 {
          // Fast path: decode 16 codes
          // 快速路径：解码 16 个编码
          out_curr += self.decode_block(block0, out_base.add(out_curr));
          out_curr += self.decode_block(block1, out_base.add(out_curr));
          in_curr += 16;
        } else if !has_esc0 {
          // First block clean, decode it
          // 第一个块无 ESC，解码它
          out_curr += self.decode_block(block0, out_base.add(out_curr));
          in_curr += 8;
        } else {
          // Slow path
          // 慢速路径
          let code = *in_ptr.add(in_curr);
          if code == ESC {
            in_curr += 1;
            if in_curr < in_end {
              *out_base.add(out_curr) = *in_ptr.add(in_curr);
              out_curr += 1;
              in_curr += 1;
            }
          } else {
            out_curr += self.decode_one(code as usize, out_base.add(out_curr));
            in_curr += 1;
          }
        }
      }

      // Handle remaining 8-15 bytes
      // 处理剩余 8-15 字节
      while in_curr + 8 <= in_end {
        let block = ptr::read_unaligned(in_ptr.add(in_curr) as *const u64);

        if !has_esc(block) {
          out_curr += self.decode_block(block, out_base.add(out_curr));
          in_curr += 8;
        } else {
          let code = *in_ptr.add(in_curr);
          if code == ESC {
            in_curr += 1;
            if in_curr < in_end {
              *out_base.add(out_curr) = *in_ptr.add(in_curr);
              out_curr += 1;
              in_curr += 1;
            }
          } else {
            out_curr += self.decode_one(code as usize, out_base.add(out_curr));
            in_curr += 1;
          }
        }
      }

      // Handle remaining bytes
      // 处理剩余字节
      while in_curr < in_end {
        let code = *in_ptr.add(in_curr);
        if code == ESC {
          in_curr += 1;
          if in_curr < in_end {
            *out_base.add(out_curr) = *in_ptr.add(in_curr);
            out_curr += 1;
            in_curr += 1;
          }
        } else {
          out_curr += self.decode_one(code as usize, out_base.add(out_curr));
          in_curr += 1;
        }
      }

      out.set_len(start + out_curr);
      out_curr
    }
  }
}
