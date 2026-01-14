use std::fmt;

use crate::{
  CODE_BASE, CODE_BITS, CODE_LEN_SHIFT_IN_CODE, CODE_MASK, CODE_MAX, HASH_TAB_SIZE, ICL_FREE,
  MAX_SYMBOL_LENGTH,
  symbol::{Symbol, hash},
};

#[derive(Clone)]
pub struct SymbolTable {
  pub short_codes: [u16; 65536],
  pub byte_codes: [u16; 256],
  pub symbols: [Symbol; CODE_MAX as usize],
  pub hash_tab: [Symbol; HASH_TAB_SIZE],
  pub n_symbols: u16,
  pub terminator: u16,
  // in a finalized symbol table, symbols are arranged by their symbol length,
  // in the order of 2, 3, 4, 5, 6, 7, 8, 1, codes < suffix_lim are 2 bytes codes that don't have a longer suffix
  pub suffix_lim: u16,
  pub len_histo: [u8; CODE_BITS as usize],
}

impl fmt::Display for SymbolTable {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f, "A FSST SymbolTable after finalize():")?;
    writeln!(f, "n_symbols: {}", self.n_symbols)?;
    for i in 0_usize..self.n_symbols as usize {
      writeln!(f, "symbols[{}]: {}", i, self.symbols[i])?;
    }
    writeln!(f, "suffix_lim: {}", self.suffix_lim)?;
    for i in 0..CODE_BITS {
      writeln!(f, "len_histo[{}]: {}", i, self.len_histo[i as usize])?;
    }
    Ok(())
  }
}

impl fmt::Debug for SymbolTable {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f, "A FSST SymbolTable before finalize():")?;
    writeln!(f, "n_symbols: {}", self.n_symbols)?;
    for i in CODE_BASE as usize..CODE_BASE as usize + self.n_symbols as usize {
      writeln!(f, "symbols[{}]: {}", i, self.symbols[i])?;
    }
    writeln!(f, "suffix_lim: {}", self.suffix_lim)?;
    for i in 0..CODE_BITS {
      writeln!(f, "len_histo[{}]: {}\n", i, self.len_histo[i as usize])?;
    }
    Ok(())
  }
}

impl SymbolTable {
  pub fn new() -> Self {
    let mut symbols = [Symbol::new(); CODE_MAX as usize];
    for (i, symbol) in symbols.iter_mut().enumerate().take(256) {
      *symbol = Symbol::from_char(i as u8, i as u16);
    }
    let unused = Symbol::from_char(0, CODE_MASK);
    for i in 256..CODE_MAX {
      symbols[i as usize] = unused;
    }
    let s = Symbol::new();
    let hash_tab = [s; HASH_TAB_SIZE];
    let mut byte_codes = [0; 256];
    for (i, byte_code) in byte_codes.iter_mut().enumerate() {
      *byte_code = i as u16;
    }
    let mut short_codes = [CODE_MASK; 65536];
    for i in 0..=65535_u16 {
      short_codes[i as usize] = i & 0xFF;
    }
    Self {
      short_codes,
      byte_codes,
      symbols,
      hash_tab,
      n_symbols: 0,
      terminator: 256,
      suffix_lim: CODE_MAX,
      len_histo: [0; CODE_BITS as usize],
    }
  }

  pub fn clear(&mut self) {
    for i in 0..256 {
      self.symbols[i] = Symbol::from_char(i as u8, i as u16);
    }
    let unused = Symbol::from_char(0, CODE_MASK);
    for i in 256..CODE_MAX {
      self.symbols[i as usize] = unused;
    }
    for i in 0..256 {
      self.byte_codes[i] = i as u16;
    }
    for i in 0..=65535_u16 {
      self.short_codes[i as usize] = i & 0xFF;
    }
    let s = Symbol::new();
    for i in 0..HASH_TAB_SIZE {
      self.hash_tab[i] = s;
    }
    for i in 0..CODE_BITS as usize {
      self.len_histo[i] = 0;
    }
    self.n_symbols = 0;
  }

  fn hash_insert(&mut self, s: Symbol) -> bool {
    let idx = (s.hash() & (HASH_TAB_SIZE as u64 - 1)) as usize;
    let taken = self.hash_tab[idx].icl < ICL_FREE;
    if taken {
      return false; // collision in hash table
    }
    self.hash_tab[idx].icl = s.icl;
    self.hash_tab[idx].val = s.val & (u64::MAX >> (s.ignored_bits()));
    true
  }

  pub fn add(&mut self, mut s: Symbol) -> bool {
    assert!(CODE_BASE + self.n_symbols < CODE_MAX);
    let len = s.symbol_len();
    s.set_code_len(CODE_BASE + self.n_symbols, len);
    if len == 1 {
      self.byte_codes[s.first() as usize] = CODE_BASE + self.n_symbols;
    } else if len == 2 {
      self.short_codes[s.first2() as usize] = CODE_BASE + self.n_symbols;
    } else if !self.hash_insert(s) {
      return false;
    }
    self.symbols[(CODE_BASE + self.n_symbols) as usize] = s;
    self.n_symbols += 1;
    self.len_histo[(len - 1) as usize] += 1;
    true
  }

  pub fn find_longest_symbol_from_char_slice(&self, input: &[u8]) -> u16 {
    let len = if input.len() >= MAX_SYMBOL_LENGTH {
      MAX_SYMBOL_LENGTH
    } else {
      input.len()
    };
    if len < 2 {
      return self.byte_codes[input[0] as usize] & CODE_MASK;
    }
    if len == 2 {
      let short_code = ((input[1] as usize) << 8) | input[0] as usize;
      if self.short_codes[short_code] >= CODE_BASE {
        return self.short_codes[short_code] & CODE_MASK;
      } else {
        return self.byte_codes[input[0] as usize] & CODE_MASK;
      }
    }
    let mut input_in_1_word = [0; 8];
    input_in_1_word[..len].copy_from_slice(&input[..len]);
    let input_in_u64 = crate::unaligned_load_unchecked(input_in_1_word.as_ptr());
    let hash_idx = hash(input_in_u64) as usize & (HASH_TAB_SIZE - 1);
    let s_in_hash_tab = self.hash_tab[hash_idx];
    if s_in_hash_tab.icl < ICL_FREE
      && s_in_hash_tab.val == (input_in_u64 & (u64::MAX >> s_in_hash_tab.ignored_bits()))
    {
      return s_in_hash_tab.code();
    }
    self.byte_codes[input[0] as usize] & CODE_MASK
  }

  // rationale for finalize:
  // - during symbol table construction, we may create more than 256 codes, but bring it down to max 255 in the last makeTable()
  //   consequently we needed more than 8 bits during symbol table construction, but can simplify the codes to single bytes in finalize()
  //   (this feature is in fact lo longer used, but could still be exploited: symbol construction creates no more than 255 symbols in each pass)
  // - we not only reduce the amount of codes to <255, but also *reorder* the symbols and renumber their codes, for higher compression perf.
  //   we renumber codes so they are grouped by length, to allow optimized scalar string compression (byteLim and suffixLim optimizations).
  // - we make the use of byteCode[] no longer necessary by inserting single-byte codes in the free spots of shortCodes[]
  //   Using shortCodes[] only makes compression faster. When creating the symbolTable, however, using shortCodes[] for the single-byte
  //   symbols is slow, as each insert touches 256 positions in it. This optimization was added when optimizing symbolTable construction time.
  //
  // In all, we change the layout and coding, as follows..
  //
  // before finalize():
  // - The real symbols are symbols[256..256+nSymbols>. As we may have nSymbols > 255
  // - The first 256 codes are pseudo symbols (all escaped bytes)
  //
  // after finalize():
  // - table layout is symbols[0..nSymbols>, with nSymbols < 256.
  // - Real codes are [0,nSymbols>. 8-th bit not set.
  // - Escapes in shortCodes have the 8th bit set (value: 256+255=511). 255 because the code to be emitted is the escape byte 255
  // - symbols are grouped by length: 2,3,4,5,6,7,8, then 1 (single-byte codes last)
  // the two-byte codes are split in two sections:
  // - first section contains codes for symbols for which there is no longer symbol (no suffix). It allows an early-out during compression
  //
  // finally, shortCodes[] is modified to also encode all single-byte symbols (hence byteCodes[] is not required on a critical path anymore).
  pub fn finalize(&mut self) {
    assert!(self.n_symbols < CODE_BASE);
    let mut new_code: [u16; 256] = [0; 256];
    let mut rsum: [u8; 8] = [0; 8];
    let byte_lim = self.n_symbols - self.len_histo[0] as u16;

    rsum[0] = byte_lim as u8; // 1-byte codes are highest
    for i in 1..7 {
      rsum[i + 1] = rsum[i] + self.len_histo[i];
    }

    let mut suffix_lim = 0;
    let mut j = rsum[2];
    for i in 0..self.n_symbols {
      let mut s1 = self.symbols[(CODE_BASE + i) as usize];
      let len = s1.symbol_len();
      let opt = if len == 2 { self.n_symbols } else { 0 };
      if opt != 0 {
        let mut has_suffix = false;
        let first2 = s1.first2();
        for k in 0..opt {
          let s2 = self.symbols[(CODE_BASE + k) as usize];
          if k != i && s2.symbol_len() > 2 && first2 == s2.first2() {
            has_suffix = true;
          }
        }
        new_code[i as usize] = if has_suffix {
          suffix_lim += 1;
          suffix_lim - 1
        } else {
          j -= 1;
          j as u16
        };
      } else {
        new_code[i as usize] = rsum[(len - 1) as usize] as u16;
        rsum[(len - 1) as usize] += 1;
      }
      s1.set_code_len(new_code[i as usize], len);
      self.symbols[new_code[i as usize] as usize] = s1;
    }

    for i in 0..256 {
      if (self.byte_codes[i] & CODE_MASK) >= CODE_BASE {
        self.byte_codes[i] =
          new_code[(self.byte_codes[i] & 0xFF) as usize] | (1 << CODE_LEN_SHIFT_IN_CODE);
      } else {
        self.byte_codes[i] = 511 | (1 << CODE_LEN_SHIFT_IN_CODE);
      }
    }

    for i in 0..65536 {
      if (self.short_codes[i] & CODE_MASK) > CODE_BASE {
        self.short_codes[i] =
          new_code[(self.short_codes[i] & 0xFF) as usize] | (2 << CODE_LEN_SHIFT_IN_CODE);
      } else {
        self.short_codes[i] = self.byte_codes[i & 0xFF] | (1 << CODE_LEN_SHIFT_IN_CODE);
      }
    }

    for i in 0..HASH_TAB_SIZE {
      if self.hash_tab[i].icl < ICL_FREE {
        self.hash_tab[i] =
          self.symbols[new_code[(self.hash_tab[i].code() & 0xFF) as usize] as usize];
      }
    }
    self.suffix_lim = suffix_lim;
  }
}

impl Default for SymbolTable {
  fn default() -> Self {
    Self::new()
  }
}
