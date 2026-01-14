use crate::{
  CODE_BASE, CODE_BITS, CODE_LEN_SHIFT_IN_CODE, CODE_MASK, CODE_MAX, HASH_TAB_SIZE, ICL_FREE,
  MAX_SYMBOL_LEN,
  symbol::{Symbol, hash},
};

mod batch;
pub mod builder;
mod fmt;

pub use batch::batch;

pub struct Table {
  pub short_codes: [u16; 65536],
  pub byte_codes: [u16; 256],
  pub symbols: [Symbol; CODE_MAX as usize],
  pub hash_tab: [Symbol; HASH_TAB_SIZE],
  pub n_symbols: u16,
  pub terminator: u16,
  pub suffix_lim: u16,
  pub len_histo: [u8; CODE_BITS as usize],
}

impl Table {
  pub fn new() -> Self {
    let mut symbols = [Symbol::new(); CODE_MAX as usize];
    for (i, sym) in symbols.iter_mut().enumerate().take(256) {
      *sym = Symbol::from_char(i as u8, i as u16);
    }
    let unused = Symbol::from_char(0, CODE_MASK);
    symbols[256..].fill(unused);

    let mut byte_codes = [0u16; 256];
    for (i, code) in byte_codes.iter_mut().enumerate() {
      *code = i as u16;
    }

    let mut short_codes = [CODE_MASK; 65536];
    for (i, code) in short_codes.iter_mut().enumerate() {
      *code = (i & 0xFF) as u16;
    }

    Self {
      short_codes,
      byte_codes,
      symbols,
      hash_tab: [Symbol::new(); HASH_TAB_SIZE],
      n_symbols: 0,
      terminator: 256,
      suffix_lim: CODE_MAX,
      len_histo: [0; CODE_BITS as usize],
    }
  }

  pub fn add(&mut self, mut s: Symbol) -> bool {
    let len = s.symbol_len();
    s.set_code_len(CODE_BASE + self.n_symbols, len);
    if len == 1 {
      self.byte_codes[s.first() as usize] = CODE_BASE + self.n_symbols;
    } else if len == 2 {
      self.short_codes[s.first2() as usize] = CODE_BASE + self.n_symbols;
    } else {
      let idx = (s.hash() & (HASH_TAB_SIZE as u64 - 1)) as usize;
      if self.hash_tab[idx].icl < ICL_FREE {
        return false;
      }
      self.hash_tab[idx].icl = s.icl;
      self.hash_tab[idx].val = s.val & (u64::MAX >> (s.ignored_bits()));
    }
    self.symbols[(CODE_BASE + self.n_symbols) as usize] = s;
    self.n_symbols += 1;
    self.len_histo[(len - 1) as usize] += 1;
    true
  }

  pub fn find_longest_symbol_from_char_slice(&self, input: &[u8]) -> u16 {
    let len = MAX_SYMBOL_LEN.min(input.len());
    if len < 2 {
      return self.byte_codes[input[0] as usize] & CODE_MASK;
    }
    if len == 2 {
      let sc = self.short_codes[((input[1] as usize) << 8) | input[0] as usize];
      return if sc >= CODE_BASE {
        sc & CODE_MASK
      } else {
        self.byte_codes[input[0] as usize] & CODE_MASK
      };
    }

    let mut input_in_1_word = [0u8; 8];
    unsafe {
      std::ptr::copy_nonoverlapping(input.as_ptr(), input_in_1_word.as_mut_ptr(), len);
    }
    let input_in_u64 = u64::from_le_bytes(input_in_1_word);

    let s = self.hash_tab[hash(input_in_u64) as usize & (HASH_TAB_SIZE - 1)];
    if s.icl < ICL_FREE && s.val == (input_in_u64 & (u64::MAX >> s.ignored_bits())) {
      return s.code();
    }
    self.byte_codes[input[0] as usize] & CODE_MASK
  }

  pub fn finalize(mut self) -> crate::encode::Encode {
    let mut new_code: [u16; 256] = [0; 256];
    let mut rsum: [u8; 8] = [0; 8];
    rsum[0] = (self.n_symbols - self.len_histo[0] as u16) as u8;
    for i in 1..7 {
      rsum[i + 1] = rsum[i] + self.len_histo[i];
    }

    let mut suffix_lim = 0u16;
    let mut j = rsum[2];
    #[allow(clippy::needless_range_loop)]
    for i in 0..self.n_symbols as usize {
      let mut s1 = self.symbols[CODE_BASE as usize + i];
      let len = s1.symbol_len();
      let code = if len == 2 {
        let first2 = s1.first2();
        if (0..self.n_symbols as usize).any(|k| {
          k != i
            && self.symbols[CODE_BASE as usize + k].symbol_len() > 2
            && first2 == self.symbols[CODE_BASE as usize + k].first2()
        }) {
          suffix_lim += 1;
          suffix_lim - 1
        } else {
          j -= 1;
          j as u16
        }
      } else {
        let c = rsum[(len - 1) as usize] as u16;
        rsum[(len - 1) as usize] += 1;
        c
      };
      new_code[i] = code;
      s1.set_code_len(code, len);
      self.symbols[code as usize] = s1;
    }

    for bc in self.byte_codes.iter_mut() {
      *bc = if (*bc & CODE_MASK) >= CODE_BASE {
        new_code[(*bc & 0xFF) as usize] | (1 << CODE_LEN_SHIFT_IN_CODE)
      } else {
        511 | (1 << CODE_LEN_SHIFT_IN_CODE)
      };
    }
    for (i, sc) in self.short_codes.iter_mut().enumerate() {
      *sc = if (*sc & CODE_MASK) > CODE_BASE {
        new_code[(*sc & 0xFF) as usize] | (2 << CODE_LEN_SHIFT_IN_CODE)
      } else {
        self.byte_codes[i & 0xFF] | (1 << CODE_LEN_SHIFT_IN_CODE)
      };
    }
    for slot in &mut self.hash_tab {
      if slot.icl < ICL_FREE {
        *slot = self.symbols[new_code[(slot.code() & 0xFF) as usize] as usize];
      }
    }

    self.suffix_lim = suffix_lim;
    self.into()
  }
}
