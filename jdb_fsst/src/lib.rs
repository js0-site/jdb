// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

mod counter;
mod decoder;
mod encoder;
mod symbol;
mod table;

use std::{io, ptr};

// Constants
// when the code is ESC, the next byte should be interpreted as is
const ESC: u8 = 255;
// when building symbol table, we have a maximum of 512 symbols, so we can use 9 bits to represent the code
const CODE_BITS: u16 = 9;
// when building symbol table, we use the first 256 codes to represent the index itself, for example, code 0 represents byte 0
const CODE_BASE: u16 = 256;

// code 512, which we can never reach(maximum code is 511)
const CODE_MAX: u16 = 1 << CODE_BITS;
// all code bits set
const CODE_MASK: u16 = CODE_MAX - 1;
// we construct FSST symbol tables using a random sample of about 16KB (1<<14)
const SAMPLETARGET: usize = 1 << 14;
const SAMPLEMAXSZ: usize = 2 * SAMPLETARGET;

// if the input size is less than 32 KB, we mark the file header and copy the input to the output as is
pub const LEAST_INPUT_SIZE: usize = 32 * 1024;

// if the max length of the input strings are less than `LEAST_INPUT_MAX_LENGTH`, we shouldn't use FSST.
pub const LEAST_INPUT_MAX_LENGTH: u64 = 5;

// we only use the lower 32 bits in icl, so we can use 1 << 32 to represent a free slot in the hash table
const ICL_FREE: u64 = 1 << 32;
// in the icl field of a symbol, the symbol length is stored in 4 bits starting from the 28th bit
const CODE_LEN_SHIFT_IN_ICL: u64 = 28;
// in the icl field of a symbol, the symbol code is stored in the 12 bits starting from the 16th bit
const CODE_SHIFT_IN_ICL: u64 = 16;

const CODE_LEN_SHIFT_IN_CODE: u64 = 12;

const HASH_TAB_SIZE: usize = 1024;

const MAX_SYMBOL_LENGTH: usize = 8;

pub const SYMBOL_TABLE_SIZE: usize = 8 + 256 * 8 + 256; // 8 bytes for the header, 256 symbols(8 bytes each), 256 bytes for lens

#[inline]
fn unaligned_load_unchecked(v: *const u8) -> u64 {
  unsafe { ptr::read_unaligned(v as *const u64) }
}

/// This is the public API for the FSST compression, when the in_buf is less than LEAST_INPUT_SIZE, we put the header and then copy the input to the output
/// we check to make sure the out_buf's size is at least the same as the in_buf's size, otherwise Err is returned, this is actually
/// risky as in some randomly generated data, the output size can be larger than the input size.
/// the out_offsets_buf should be at least the same size as the in_offsets_buf, otherwise Err is returned
/// the symbol_table is used to store the symbol table created by `compression`, it's size should be SYMBOL_TABLE_SIZE
/// after compression, the first 64 bits of the output buffer is the fsst header:
/// from most significant bit to least significant bit:
/// | encoder_switch |    suffix_lim | terminator | n_symbols
/// |         8 bits |        8 bits |     8 bits | 8 bits
/// then followed by the compressed data
///
pub fn compress(
  symbol_table: &mut [u8],
  in_buf: &[u8],
  in_offsets_buf: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<usize>,
) -> io::Result<()> {
  encoder::FsstEncoder::new().compress(
    in_buf,
    in_offsets_buf,
    out_buf,
    out_offsets_buf,
    symbol_table,
  )?;
  Ok(())
}

// This is the public API for the FSST decompression
// when the decoder_switch_on is off in the in_buf header, `decompress` first make sure the out_buf is at least the same size as the in_buf, then simply copy the
// input data to the output
// when the decoder_switch_on is on, `decompress` first make sure the out_buf is at least 3 times the size of the in_buf, then start decoding the
// data using the symbol table
// the out_offsets_buf should be at least the same size as the in_offsets_buf, otherwise an error is returned
// the symbol_table is the same symbol table created by `compression`
pub fn decompress(
  symbol_table: &[u8],
  in_buf: &[u8],
  in_offsets_buf: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<usize>,
) -> io::Result<()> {
  let mut dec = decoder::FsstDecoder::new();
  dec.init(
    symbol_table,
    in_buf,
    in_offsets_buf,
    out_buf,
    out_offsets_buf,
  )?;
  dec.decompress(in_buf, in_offsets_buf, out_buf, out_offsets_buf)?;
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_symbol_new() {
    let st = table::SymbolTable::new();
    assert!(st.n_symbols == 0);
    for i in 0..=255_u8 {
      assert!(st.symbols[i as usize] == symbol::Symbol::from_char(i, i as u16));
    }
    let s = symbol::Symbol::from_char(1, 1);
    assert!(s == st.symbols[1]);
    for i in 0..HASH_TAB_SIZE {
      assert!(st.hash_tab[i] == symbol::Symbol::new());
    }
  }
}
