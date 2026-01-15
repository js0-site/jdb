use std::{borrow::Borrow, io};

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
  /// Decode a single compressed byte slice.
  /// 解码单个压缩字节切片。
  pub fn decode(&self, bin: impl AsRef<[u8]>, out: &mut Vec<u8>) -> io::Result<usize> {
    let data = bin.as_ref();
    if data.is_empty() {
      return Ok(0);
    }

    if self.n_symbols == 0 {
      out.extend_from_slice(data);
      return Ok(data.len());
    }

    let symbol = &self.symbol;
    let len = &self.len;
    let start_len = out.len();

    let mut in_curr = 0;
    while in_curr < data.len() {
      let code = data[in_curr] as usize;

      if code == ESC as usize {
        // Escape byte, next byte is literal
        // 转义字节，下一个字节是字面值
        in_curr += 1;
        if in_curr < data.len() {
          out.push(data[in_curr]);
          in_curr += 1;
        }
      } else {
        // Regular symbol
        // 常规符号
        let l = len[code] as usize;
        if l == 0 {
          // Invalid code, treat as literal
          // 无效编码，当作字面值
          out.push(code as u8);
          in_curr += 1;
          continue;
        }

        let s = symbol[code];

        // Write symbol bytes one by one to avoid overwriting
        // 逐字节写入符号以避免覆盖
        for i in 0..l {
          out.push((s >> (i * 8)) as u8);
        }
        in_curr += 1;
      }
    }

    Ok(out.len() - start_len)
  }
}
