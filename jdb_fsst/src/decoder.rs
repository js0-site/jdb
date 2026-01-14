use std::{io, ptr};

use crate::{ESC, SYMBOL_TABLE_SIZE, unaligned_load_unchecked};

const CORRUPT: u64 = 32774747032022883; // 7-byte number in little endian containing "corrupt"

pub struct FsstDecoder {
  lens: [u8; 256],
  symbols: [u64; 256],
  decoder_switch_on: bool,
}

impl FsstDecoder {
  pub fn new() -> Self {
    Self {
      lens: [0; 256],
      symbols: [CORRUPT; 256],
      decoder_switch_on: false,
    }
  }

  pub fn init(
    &mut self,
    symbol_table: &[u8],
    in_buf: &[u8],
    in_offsets_buf: &[usize],
    out_buf: &[u8],
    out_offsets_buf: &[usize],
  ) -> io::Result<()> {
    let st_info = u64::from_ne_bytes(symbol_table[..8].try_into().unwrap());

    if symbol_table.len() != SYMBOL_TABLE_SIZE {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "the symbol table buffer for FSST decoder must have size {}",
          SYMBOL_TABLE_SIZE
        ),
      ));
    }

    self.decoder_switch_on = (st_info & (1 << 24)) != 0;
    // when decoder_switch_on is true, we make sure the out_buf is at least 3 times the size of the in_buf,
    if self.decoder_switch_on && in_buf.len() * 3 > out_buf.len() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "output buffer too small for FSST decoder",
      ));
    }

    // when decoder_switch_on is false, we make sure the out_buf is at least the same size of the in_buf,
    if !self.decoder_switch_on && in_buf.len() > out_buf.len() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "output buffer too small for FSST decoder",
      ));
    }

    if in_offsets_buf.len() > out_offsets_buf.len() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "output offsets buffer ({}) too small for FSST decoder (need at least {})",
          out_offsets_buf.len(),
          in_offsets_buf.len()
        ),
      ));
    }
    let symbol_num = (st_info & 255) as u8;
    let mut pos = 8;
    for i in 0..symbol_num as usize {
      self.symbols[i] = unaligned_load_unchecked(symbol_table[pos..].as_ptr());
      pos += 8;
    }
    for i in 0..symbol_num as usize {
      self.lens[i] = symbol_table[pos];
      pos += 1;
    }
    Ok(())
  }

  pub fn decompress(
    &mut self,
    in_buf: &[u8],
    in_offsets_buf: &[usize],
    out_buf: &mut Vec<u8>,
    out_offsets_buf: &mut Vec<usize>,
  ) -> io::Result<()> {
    if !self.decoder_switch_on {
      out_buf.resize(in_buf.len(), 0);
      out_buf.copy_from_slice(in_buf);
      out_offsets_buf.resize(in_offsets_buf.len(), 0);
      out_offsets_buf.copy_from_slice(in_offsets_buf);
      return Ok(());
    }
    let mut out_pos = 0;
    let mut out_offsets_len = 0;
    decompress_bulk(
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

impl Default for FsstDecoder {
  fn default() -> Self {
    Self::new()
  }
}

fn decompress_bulk(
  decoder: &FsstDecoder,
  compressed_strs: &[u8],
  offsets: &[usize],
  out: &mut Vec<u8>,
  out_offsets: &mut Vec<usize>,
  out_pos: &mut usize,
  out_offsets_len: &mut usize,
) -> io::Result<()> {
  let symbols = decoder.symbols;
  let lens = decoder.lens;
  let mut decompress = |mut in_curr: usize, in_end: usize, out_curr: &mut usize| {
    // Do SIMD operation here by 4 bytes
    while in_curr + 4 <= in_end {
      let next_block;
      let mut code;
      let mut len;
      unsafe {
        next_block = ptr::read_unaligned(compressed_strs.as_ptr().add(in_curr) as *const u32);
      }
      let escape_mask = (next_block & 0x80808080u32)
        & ((((!next_block) & 0x7F7F7F7Fu32) + 0x7F7F7F7Fu32) ^ 0x80808080u32);
      if escape_mask == 0 {
        // 0th byte
        code = compressed_strs[in_curr] as usize;
        len = lens[code] as usize;
        unsafe {
          let src = symbols[code];
          ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
        }
        in_curr += 1;
        *out_curr += len;

        // 1st byte
        code = compressed_strs[in_curr] as usize;
        len = lens[code] as usize;
        unsafe {
          let src = symbols[code];
          ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
        }
        in_curr += 1;
        *out_curr += len;

        // 2nd byte
        code = compressed_strs[in_curr] as usize;
        len = lens[code] as usize;
        unsafe {
          let src = symbols[code];
          ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
        }
        in_curr += 1;
        *out_curr += len;

        // 3rd byte
        code = compressed_strs[in_curr] as usize;
        len = lens[code] as usize;
        unsafe {
          let src = symbols[code];
          ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
        }
        in_curr += 1;
        *out_curr += len;
      } else {
        let first_escape_pos = escape_mask.trailing_zeros() >> 3;
        if first_escape_pos == 3 {
          // 0th byte
          code = compressed_strs[in_curr] as usize;
          len = lens[code] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += len;

          // 1st byte
          code = compressed_strs[in_curr] as usize;
          len = lens[code] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += len;

          // 2nd byte
          code = compressed_strs[in_curr] as usize;
          len = lens[code] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += len;

          // escape byte
          in_curr += 2;
          out[*out_curr] = compressed_strs[in_curr - 1];
          *out_curr += 1;
        } else if first_escape_pos == 2 {
          // 0th byte
          code = compressed_strs[in_curr] as usize;
          len = lens[code] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += len;

          // 1st byte
          code = compressed_strs[in_curr] as usize;
          len = lens[code] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += len;

          // escape byte
          in_curr += 2;
          out[*out_curr] = compressed_strs[in_curr - 1];
          *out_curr += 1;
        } else if first_escape_pos == 1 {
          // 0th byte
          code = compressed_strs[in_curr] as usize;
          len = lens[code] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += len;

          // escape byte
          in_curr += 2;
          out[*out_curr] = compressed_strs[in_curr - 1];
          *out_curr += 1;
        } else {
          // escape byte
          in_curr += 2;
          out[*out_curr] = compressed_strs[in_curr - 1];
          *out_curr += 1;
        }
      }
    }

    // handle the remaining bytes
    if in_curr + 2 <= in_end {
      out[*out_curr] = compressed_strs[in_curr + 1];
      if compressed_strs[in_curr] != ESC {
        let code = compressed_strs[in_curr] as usize;
        unsafe {
          let src = symbols[code];
          ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
        }
        in_curr += 1;
        *out_curr += lens[code] as usize;
        if compressed_strs[in_curr] != ESC {
          let code = compressed_strs[in_curr] as usize;
          unsafe {
            let src = symbols[code];
            ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
          }
          in_curr += 1;
          *out_curr += lens[code] as usize;
        } else {
          in_curr += 2;
          out[*out_curr] = compressed_strs[in_curr - 1];
          *out_curr += 1;
        }
      } else {
        in_curr += 2;
        *out_curr += 1;
      }
    }

    if in_curr < in_end {
      // last code cannot be an escape code
      let code = compressed_strs[in_curr] as usize;
      unsafe {
        let src = symbols[code];
        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
      }
      *out_curr += lens[code] as usize;
    }
  };

  let mut out_curr = *out_pos;
  out_offsets[0] = *out_pos;
  for i in 1..offsets.len() {
    let in_curr = offsets[i - 1];
    let in_end = offsets[i];
    decompress(in_curr, in_end, &mut out_curr);
    out_offsets[i] = out_curr;
  }
  out.resize(out_curr, 0);
  out_offsets.resize(offsets.len(), 0);
  *out_pos = out_curr;
  *out_offsets_len = offsets.len();
  Ok(())
}
