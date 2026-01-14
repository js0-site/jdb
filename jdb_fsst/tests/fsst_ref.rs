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

// we only use the lower 32 bits in icl, so we can use 1 << 32 to represent a free slot in the hash table
const ICL_FREE: u64 = 1 << 32;
// in the icl field of a symbol, the symbol length is stored in 4 bits starting from the 28th bit
const CODE_LEN_SHIFT_IN_ICL: u64 = 28;
// in the icl field of a symbol, the symbol code is stored in the 12 bits starting from the 16th bit
const CODE_SHIFT_IN_ICL: u64 = 16;

const CODE_LEN_SHIFT_IN_CODE: u64 = 12;

const HASH_TAB_SIZE: usize = 1024;
const HASH_PRIME: u64 = 2971215073;
const SHIFT: usize = 15;
#[inline]
fn fsst_hash(w: u64) -> u64 {
  w.wrapping_mul(HASH_PRIME) ^ ((w.wrapping_mul(HASH_PRIME)) >> SHIFT)
}

const MAX_SYMBOL_LENGTH: usize = 8;

// use arrow_array::OffsetSizeTrait;
// use rand::rngs::StdRng;
// use rand::{Rng, SeedableRng};
use std::{
  cmp::Ordering,
  collections::{BinaryHeap, HashSet},
  io, ptr,
};

#[inline]
fn fsst_unaligned_load_unchecked(v: *const u8) -> u64 {
  unsafe { ptr::read_unaligned(v as *const u64) }
}
#[derive(Default, Copy, Clone, PartialEq, Eq)]
pub struct Symbol {
  // the byte sequence that this symbol stands for
  val: u64,

  // icl = u64 ignoredBits:16,code:12,length:4,unused:32 -- but we avoid exposing this bit-field notation
  // use a single u64 to be sure "code" is accessed with one load and can be compared with one comparison
  icl: u64,
}

use std::fmt;

impl fmt::Display for Symbol {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let bytes = self.val.to_ne_bytes();
    for i in 0..self.symbol_len() {
      write!(f, "{}", bytes[i as usize] as char)?;
    }
    write!(f, "\t")?;
    write!(
      f,
      "ignoredBits: {}, code: {}, length: {}",
      self.ignored_bits(),
      self.code(),
      self.symbol_len()
    )?;
    Ok(())
  }
}

impl fmt::Debug for Symbol {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let bytes = self.val.to_ne_bytes();
    for i in 0..self.symbol_len() {
      write!(f, "{}", bytes[i as usize] as char)?;
    }
    write!(f, "\t")?;
    write!(
      f,
      "ignoredBits: {}, code: {}, length: {}",
      self.ignored_bits(),
      self.code(),
      self.symbol_len()
    )?;
    Ok(())
  }
}

impl Symbol {
  fn new() -> Self {
    Self {
      val: 0,
      icl: ICL_FREE,
    }
  }

  fn from_char(c: u8, code: u16) -> Self {
    Self {
      val: c as u64,
      // in a symbol which represents a single character, 56 bits(7 bytes) are ignored, code length is 1
      icl: (1 << CODE_LEN_SHIFT_IN_ICL) | ((code as u64) << CODE_SHIFT_IN_ICL) | 56,
    }
  }

  fn set_code_len(&mut self, code: u16, len: u32) {
    self.icl = ((len as u64) << CODE_LEN_SHIFT_IN_ICL)
      | ((code as u64) << CODE_SHIFT_IN_ICL)
      | ((8u64.saturating_sub(len as u64)) * 8);
  }

  #[inline]
  fn symbol_len(&self) -> u32 {
    (self.icl >> CODE_LEN_SHIFT_IN_ICL) as u32
  }

  #[inline]
  fn code(&self) -> u16 {
    ((self.icl >> CODE_SHIFT_IN_ICL) & CODE_MASK as u64) as u16
  }

  // ignoredBits is (8-length)*8, which is the amount of high bits to zero in the input word before comparing with the hashtable key
  // it could of course be computed from len during lookup, but storing it precomputed in some loose bits is faster
  #[inline]
  fn ignored_bits(&self) -> u32 {
    (self.icl & u16::MAX as u64) as u32
  }

  #[inline]
  fn first(&self) -> u8 {
    assert!(self.symbol_len() >= 1);
    (0xFF & self.val) as u8
  }

  #[inline]
  fn first2(&self) -> u16 {
    assert!(self.symbol_len() >= 2);
    (0xFFFF & self.val) as u16
  }

  #[inline]
  fn hash(&self) -> u64 {
    let v = 0xFFFFFF & self.val;
    fsst_hash(v)
  }

  // right is the substring follows left
  // for example, in "hello",
  // "llo" is the substring that follows "he"
  fn concat(left: Self, right: Self) -> Self {
    let mut s = Self::new();
    let mut length = left.symbol_len() + right.symbol_len();
    if length > MAX_SYMBOL_LENGTH as u32 {
      length = MAX_SYMBOL_LENGTH as u32;
    }
    s.set_code_len(CODE_MASK, length);
    s.val = (right.val << (8 * left.symbol_len())) | left.val;
    s
  }
}

// Symbol that can be put in a queue, ordered on gain
#[derive(Clone)]
struct QSymbol {
  symbol: Symbol,
  // the gain field is only used in the symbol queue that sorts symbols on gain
  gain: u32,
}

impl PartialEq for QSymbol {
  fn eq(&self, other: &Self) -> bool {
    self.symbol.val == other.symbol.val && self.symbol.icl == other.symbol.icl
  }
}

impl Ord for QSymbol {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .gain
      .cmp(&other.gain)
      .then_with(|| other.symbol.val.cmp(&self.symbol.val))
  }
}

impl PartialOrd for QSymbol {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Eq for QSymbol {}

use std::hash::{Hash, Hasher};

impl Hash for QSymbol {
  // this hash algorithm follows the C++ implementation of the FSST in the paper
  fn hash<H: Hasher>(&self, state: &mut H) {
    let mut k = self.symbol.val;
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h: u64 = 0x8445d61a4e774912 ^ (8u64.wrapping_mul(M));
    k = k.wrapping_mul(M);
    k ^= k >> R;
    k = k.wrapping_mul(M);
    h ^= k;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h.hash(state);
  }
}

#[derive(Clone)]
pub struct Table {
  pub short_codes: [u16; 65536],
  pub byte_codes: [u16; 256],
  pub symbols: [Symbol; CODE_MAX as usize],
  hash_tab: [Symbol; HASH_TAB_SIZE],
  pub n_symbols: u16,
  pub terminator: u16,
  // in a finalized symbol table, symbols are arranged by their symbol length,
  // in the order of 2, 3, 4, 5, 6, 7, 8, 1, codes < suffix_lim are 2 bytes codes that don't have a longer suffix
  suffix_lim: u16,
  len_histo: [u8; CODE_BITS as usize],
}

impl std::fmt::Display for Table {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    writeln!(f, "A FSST Table after finalize():")?;
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

impl std::fmt::Debug for Table {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    writeln!(f, "A FSST Table before finalize():")?;
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

impl Table {
  fn new() -> Self {
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

  fn clear(&mut self) {
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

  fn add(&mut self, mut s: Symbol) -> bool {
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

  fn find_longest_symbol_from_char_slice(&self, input: &[u8]) -> u16 {
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
    let input_in_u64 = fsst_unaligned_load_unchecked(input_in_1_word.as_ptr());
    let hash_idx = fsst_hash(input_in_u64) as usize & (HASH_TAB_SIZE - 1);
    let s_in_hash_tab = self.hash_tab[hash_idx];
    if s_in_hash_tab.icl < ICL_FREE
      && s_in_hash_tab.val == (input_in_u64 & (u64::MAX >> s_in_hash_tab.ignored_bits()))
    {
      return s_in_hash_tab.code();
    }
    self.byte_codes[input[0] as usize] & CODE_MASK
  }

  fn finalize(&mut self) {
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

#[derive(Clone)]
struct Counters {
  count1: Vec<u16>,
  count2: Vec<Vec<u16>>,
}

impl Counters {
  fn new() -> Self {
    Self {
      count1: vec![0; CODE_MAX as usize],
      count2: vec![vec![0; CODE_MAX as usize]; CODE_MAX as usize],
    }
  }

  #[inline]
  fn count1_set(&mut self, pos1: usize, val: u16) {
    self.count1[pos1] = val;
  }

  #[inline]
  fn count1_inc(&mut self, pos1: u16) {
    self.count1[pos1 as usize] = self.count1[pos1 as usize].saturating_add(1);
  }

  #[inline]
  fn count2_inc(&mut self, pos1: usize, pos2: usize) {
    self.count2[pos1][pos2] = self.count2[pos1][pos2].saturating_add(1);
  }

  #[inline]
  fn count1_get(&self, pos1: usize) -> u16 {
    self.count1[pos1]
  }

  #[inline]
  fn count2_get(&self, pos1: usize, pos2: usize) -> u16 {
    self.count2[pos1][pos2]
  }
}

#[inline]
fn is_escape_code(pos: u16) -> bool {
  pos < CODE_BASE
}

// make_sample selects strings randoms from the input, and returns a set of strings of size around SAMPLETARGET
fn make_sample(in_buf: &[u8], offsets: &[usize]) -> (Vec<u8>, Vec<usize>) {
  let total_size = in_buf.len();
  if total_size <= SAMPLETARGET {
    return (in_buf.to_vec(), offsets.to_vec());
  }
  let mut sample_buf = Vec::with_capacity(SAMPLEMAXSZ);
  let mut sample_offsets: Vec<usize> = Vec::new();

  sample_offsets.push(0);
  // let mut rng = StdRng::from_os_rng();
  while sample_buf.len() < SAMPLETARGET {
    let rand_num = fastrand::usize(0..offsets.len() - 1);
    sample_buf.extend_from_slice(&in_buf[offsets[rand_num]..offsets[rand_num + 1]]);
    sample_offsets.push(sample_buf.len());
  }
  sample_offsets.push(sample_buf.len());
  (sample_buf, sample_offsets)
}

// build_symbol_table constructs a symbol table from a sample of the input
pub fn build_symbol_table(
  sample_buf: Vec<u8>,
  sample_offsets: Vec<usize>,
) -> io::Result<Box<Table>> {
  let mut st = Table::new();
  let mut best_table = Table::new();
  // worst case (everything exception), will be updated later
  let mut best_gain = 0isize - SAMPLEMAXSZ as isize;

  let mut byte_histo = [0; 256];
  for c in &sample_buf {
    byte_histo[*c as usize] += 1;
  }
  let mut curr_min_histo = SAMPLEMAXSZ;

  for (i, this_byte_histo) in byte_histo.iter().enumerate() {
    if *this_byte_histo < curr_min_histo {
      curr_min_histo = *this_byte_histo;
      st.terminator = i as u16;
    }
  }

  // Compress sample, and compute (pair-)frequencies
  let compress_count = |st: &mut Table, sample_frac: usize| -> (Box<Counters>, isize) {
    let mut gain = 0isize;
    let mut counters = Counters::new();

    for i in 1..sample_offsets.len() {
      if sample_offsets[i] == sample_offsets[i - 1] {
        continue;
      }
      let word = &sample_buf[sample_offsets[i - 1]..sample_offsets[i]];

      let mut curr = 0;
      let mut curr_code;
      let mut prev_code = st.find_longest_symbol_from_char_slice(&word[curr..]);
      curr += st.symbols[prev_code as usize].symbol_len() as usize;

      // Avoid arithmetic on Option<T>
      let symbol_len = st.symbols[prev_code as usize].symbol_len() as usize;
      let escape_cost = if is_escape_code(prev_code) { 1 } else { 0 };
      let gain_contribution = symbol_len.saturating_sub(1 + escape_cost);
      gain += gain_contribution as isize;

      while curr < word.len() {
        counters.count1_inc(prev_code);
        let symbol_len;

        if st.symbols[prev_code as usize].symbol_len() != 1 {
          counters.count1_inc(word[curr] as u16);
        }

        if word.len() > 7 && curr < word.len() - 7 {
          let mut this_64_bit_word: u64 = fsst_unaligned_load_unchecked(word[curr..].as_ptr());
          let code = this_64_bit_word & 0xFFFFFF;
          let idx = fsst_hash(code) as usize & (HASH_TAB_SIZE - 1);
          let s: Symbol = st.hash_tab[idx];
          let short_code = st.short_codes[(this_64_bit_word & 0xFFFF) as usize] & CODE_MASK;
          this_64_bit_word &= 0xFFFFFFFFFFFFFFFF >> s.icl as u8;
          if (s.icl < ICL_FREE) & (s.val == this_64_bit_word) {
            curr_code = s.code();
            symbol_len = s.symbol_len();
          } else if short_code >= CODE_BASE {
            curr_code = short_code;
            symbol_len = 2;
          } else {
            curr_code = st.byte_codes[(this_64_bit_word & 0xFF) as usize] & CODE_MASK;
            symbol_len = 1;
          }
        } else {
          curr_code = st.find_longest_symbol_from_char_slice(&word[curr..]);
          symbol_len = st.symbols[curr_code as usize].symbol_len();
        }

        // Avoid arithmetic on Option<T>
        let symbol_len_usize = symbol_len as usize;
        let escape_cost = if is_escape_code(curr_code) { 1 } else { 0 };
        let gain_contribution = symbol_len_usize.saturating_sub(1 + escape_cost);
        gain += gain_contribution as isize;

        // no need to count pairs in final round
        if sample_frac < 128 {
          // consider the symbol that is the concatenation of the last two symbols
          counters.count2_inc(prev_code as usize, curr_code as usize);
          if symbol_len > 1 {
            counters.count2_inc(prev_code as usize, word[curr] as usize);
          }
        }
        curr += symbol_len as usize;
        prev_code = curr_code;
      }
      counters.count1_inc(prev_code);
    }
    (Box::new(counters), gain)
  };

  let make_table = |st: &mut Table, counters: &mut Counters, sample_frac: usize| {
    let mut candidates: HashSet<QSymbol> = HashSet::new();

    counters.count1_set(st.terminator as usize, u16::MAX);

    let add_or_inc = |cands: &mut HashSet<QSymbol>, s: Symbol, count: u64| {
      if count < (5 * sample_frac as u64) / 128 {
        return;
      }
      let mut q = QSymbol {
        symbol: s,
        gain: (count * s.symbol_len() as u64) as u32,
      };
      if let Some(old_q) = cands.get(&q) {
        q.gain += old_q.gain;
        cands.remove(&old_q.clone());
      }
      cands.insert(q);
    };

    // add candidate symbols based on counted frequencies
    for pos1 in 0..CODE_BASE as usize + st.n_symbols as usize {
      let cnt1 = counters.count1_get(pos1);
      if cnt1 == 0 {
        continue;
      }
      // heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases [de]compression speed
      let s1 = st.symbols[pos1];
      add_or_inc(
        &mut candidates,
        s1,
        if s1.symbol_len() == 1 { 8 } else { 1 } * cnt1 as u64,
      );
      if s1.first() == st.terminator as u8 {
        continue;
      }
      if sample_frac >= 128
        || s1.symbol_len() == MAX_SYMBOL_LENGTH as u32
        || s1.first() == st.terminator as u8
      {
        continue;
      }
      for pos2 in 0..CODE_BASE as usize + st.n_symbols as usize {
        let cnt2 = counters.count2_get(pos1, pos2);
        if cnt2 == 0 {
          continue;
        }

        // create a new symbol
        let s2 = st.symbols[pos2];
        let s3 = Symbol::concat(s1, s2);
        // multi-byte symbols cannot contain the terminator byte
        if s2.first() != st.terminator as u8 {
          add_or_inc(&mut candidates, s3, cnt2 as u64);
        }
      }
    }
    let mut pq: BinaryHeap<QSymbol> = BinaryHeap::new();
    for q in &candidates {
      pq.push(q.clone());
    }

    // Create new symbol map using best candidates
    st.clear();
    while st.n_symbols < 255 && !pq.is_empty() {
      let q = pq.pop().unwrap();
      st.add(q.symbol);
    }
  };

  for frac in [8, 38, 68, 98, 108, 128] {
    // we do 5 rounds (sampleFrac=8,38,68,98,128)
    let (mut this_counter, gain) = compress_count(&mut st, frac);
    if gain >= best_gain {
      // a new best solution
      best_gain = gain;
      best_table = st.clone();
    }
    make_table(&mut st, &mut this_counter, frac);
  }
  best_table.finalize(); // renumber codes for more efficient compression
  if best_table.n_symbols == 0 {
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      format!(
        " failed to build symbol table, input len: {}, input_offsets len: {}",
        sample_buf.len(),
        sample_offsets.len()
      ),
    ));
  }
  Ok(Box::new(best_table))
}

pub fn compress_bulk(
  st: &Table,
  strs: &[u8],
  offsets: &[usize],
  out: &mut Vec<u8>,
  out_offsets: &mut Vec<usize>,
  out_pos: &mut usize,
  out_offsets_len: &mut usize,
) -> io::Result<()> {
  let mut out_curr = *out_pos;

  let mut compress = |buf: &[u8], in_end: usize, out_curr: &mut usize| {
    let mut in_curr = 0;
    while in_curr < in_end {
      let word = fsst_unaligned_load_unchecked(buf[in_curr..].as_ptr());
      let short_code = st.short_codes[(word & 0xFFFF) as usize];
      let word_first_3_byte = word & 0xFFFFFF;
      let idx = fsst_hash(word_first_3_byte) as usize & (HASH_TAB_SIZE - 1);
      let s = st.hash_tab[idx];
      out[*out_curr + 1] = word as u8; // speculatively write out escaped byte
      let code = if s.icl < ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
        (s.icl >> 16) as u16
      } else {
        short_code
      };
      out[*out_curr] = code as u8;
      in_curr += (code >> 12) as usize;
      *out_curr += 1 + ((code & 256) >> 8) as usize;
    }
  };

  out_offsets[0] = *out_pos;
  for i in 1..offsets.len() {
    let mut in_curr = offsets[i - 1];
    let end_curr = offsets[i];
    let mut buf: [u8; 520] = [0; 520]; // +8 sentinel is to avoid 8-byte unaligned-loads going beyond 511 out-of-bounds
    while in_curr < end_curr {
      let in_end = std::cmp::min(in_curr + 511, end_curr);
      {
        let this_len = in_end - in_curr;
        buf[..this_len].copy_from_slice(&strs[in_curr..in_end]);
        buf[this_len] = st.terminator as u8; // sentinel
      }
      compress(&buf, in_end - in_curr, &mut out_curr);
      in_curr = in_end;
    }
    out_offsets[i] = out_curr;
  }

  out.resize(out_curr, 0); // shrink to actual size
  out_offsets.resize(offsets.len(), 0); // shrink to actual size
  *out_pos = out_curr;
  *out_offsets_len = offsets.len();
  Ok(())
}

pub fn compress(in_buf: &[u8], offsets: &[usize]) -> Vec<u8> {
  let (sample, sample_offsets) = make_sample(in_buf, offsets);
  let st = build_symbol_table(sample, sample_offsets).unwrap();
  let mut out = vec![0; in_buf.len() * 2];
  let mut out_offsets = vec![0; offsets.len()];
  let mut out_pos = 0;
  let mut out_offsets_len = 0;
  compress_bulk(
    &st,
    in_buf,
    offsets,
    &mut out,
    &mut out_offsets,
    &mut out_pos,
    &mut out_offsets_len,
  )
  .unwrap();
  out
}
