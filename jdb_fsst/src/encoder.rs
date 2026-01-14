use std::{
  collections::{BinaryHeap, HashSet},
  io,
};

use crate::{
  CODE_BASE, CODE_MASK, HASH_TAB_SIZE, LEAST_INPUT_SIZE, MAX_SYMBOL_LENGTH, SAMPLEMAXSZ,
  SAMPLETARGET, SYMBOL_TABLE_SIZE,
  counter::Counters,
  symbol::{QSymbol, Symbol, hash},
  table::SymbolTable,
  unaligned_load_unchecked,
};

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
  while sample_buf.len() < SAMPLETARGET {
    let rand_num = fastrand::usize(0..offsets.len() - 1);
    sample_buf.extend_from_slice(&in_buf[offsets[rand_num]..offsets[rand_num + 1]]);
    sample_offsets.push(sample_buf.len());
  }
  sample_offsets.push(sample_buf.len());
  (sample_buf, sample_offsets)
}

// build_symbol_table constructs a symbol table from a sample of the input
fn build_symbol_table(
  sample_buf: Vec<u8>,
  sample_offsets: Vec<usize>,
) -> io::Result<Box<SymbolTable>> {
  let mut st = SymbolTable::new();
  let mut best_table = SymbolTable::new();
  // worst case (everything exception), will be updated later
  let mut best_gain: isize = -(SAMPLEMAXSZ as isize);

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
  let compress_count = |st: &mut SymbolTable, sample_frac: usize| -> (Box<Counters>, isize) {
    let mut gain: isize = 0;
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
          let mut this_64_bit_word: u64 = unaligned_load_unchecked(word[curr..].as_ptr());
          let code = this_64_bit_word & 0xFFFFFF;
          let idx = hash(code) as usize & (HASH_TAB_SIZE - 1);
          let s: Symbol = st.hash_tab[idx];
          let short_code = st.short_codes[(this_64_bit_word & 0xFFFF) as usize] & CODE_MASK;
          this_64_bit_word &= 0xFFFFFFFFFFFFFFFF >> s.icl as u8;
          if (s.icl < crate::ICL_FREE) & (s.val == this_64_bit_word) {
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

  let make_table = |st: &mut SymbolTable, counters: &mut Counters, sample_frac: usize| {
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
        "Fsst failed to build symbol table, input len: {}, input_offsets len: {}",
        sample_buf.len(),
        sample_offsets.len()
      ),
    ));
  }
  Ok(Box::new(best_table))
}

pub fn compress_bulk(
  st: &SymbolTable,
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
      let word = unaligned_load_unchecked(buf[in_curr..].as_ptr());
      let short_code = st.short_codes[(word & 0xFFFF) as usize];
      let word_first_3_byte = word & 0xFFFFFF;
      let idx = hash(word_first_3_byte) as usize & (HASH_TAB_SIZE - 1);
      let s = st.hash_tab[idx];
      out[*out_curr + 1] = word as u8; // speculatively write out escaped byte
      let code = if s.icl < crate::ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
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

pub struct FsstEncoder {
  symbol_table: Box<SymbolTable>,
  // when in_buf is less than LEAST_INPUT_SIZE, we simply copy the input to the output
  encoder_switch: bool,
}

impl FsstEncoder {
  pub fn new() -> Self {
    Self {
      symbol_table: Box::new(SymbolTable::new()),
      encoder_switch: false,
    }
  }

  fn init(
    &mut self,
    in_buf: &[u8],
    in_offsets_buf: &[usize],
    out_buf: &[u8],
    out_offsets_buf: &[usize],
    symbol_table: &[u8],
  ) -> io::Result<()> {
    if symbol_table.len() != SYMBOL_TABLE_SIZE {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "the symbol table buffer for FSST encoder must have size {}",
          SYMBOL_TABLE_SIZE
        ),
      ));
    }

    if in_buf.len() < LEAST_INPUT_SIZE {
      return Ok(());
    }

    // currently, we make sure the compression output buffer has at least the same size as the input buffer,
    if in_buf.len() > out_buf.len() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "output buffer ({}) too small for FSST encoder (need at least {})",
          out_buf.len(),
          in_buf.len()
        ),
      ));
    }
    if in_offsets_buf.len() > out_offsets_buf.len() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "output offsets buffer ({}) too small for FSST encoder (need at least {})",
          out_offsets_buf.len(),
          in_offsets_buf.len()
        ),
      ));
    }

    self.encoder_switch = true;
    let (sample, sample_offsets) = make_sample(in_buf, in_offsets_buf);
    let st = build_symbol_table(sample, sample_offsets)?;
    self.symbol_table = st;
    Ok(())
  }

  fn export(&self, symbol_table_buf: &mut [u8]) -> io::Result<()> {
    let st = &self.symbol_table;

    let st_info: u64 = ((self.encoder_switch as u64) << 24)
      | (((st.suffix_lim & 255) as u64) << 16)
      | (((st.terminator & 255) as u64) << 8)
      | ((st.n_symbols & 255) as u64);

    let st_info_bytes = st_info.to_ne_bytes();
    let mut pos = 0;
    symbol_table_buf[pos..pos + st_info_bytes.len()].copy_from_slice(&st_info_bytes);

    pos += st_info_bytes.len();

    for i in 0..st.n_symbols as usize {
      let s = st.symbols[i];
      let s_bytes = s.val.to_ne_bytes();
      symbol_table_buf[pos..pos + s_bytes.len()].copy_from_slice(&s_bytes);
      pos += s_bytes.len();
    }
    for i in 0..st.n_symbols as usize {
      let this_len = st.symbols[i].symbol_len();
      symbol_table_buf[pos] = this_len as u8;
      pos += 1;
    }
    Ok(())
  }

  pub fn compress(
    &mut self,
    in_buf: &[u8],
    in_offsets_buf: &[usize],
    out_buf: &mut Vec<u8>,
    out_offsets_buf: &mut Vec<usize>,
    symbol_table_buf: &mut [u8],
  ) -> io::Result<()> {
    self.init(
      in_buf,
      in_offsets_buf,
      out_buf,
      out_offsets_buf,
      symbol_table_buf,
    )?;
    self.export(symbol_table_buf)?;

    // if the input buffer is less than LEAST_INPUT_SIZE, we simply copy the input to the output
    if !self.encoder_switch {
      out_buf.resize(in_buf.len(), 0);
      out_buf.copy_from_slice(in_buf);
      out_offsets_buf.resize(in_offsets_buf.len(), 0);
      out_offsets_buf.copy_from_slice(in_offsets_buf);
      return Ok(());
    }
    let mut out_pos = 0;
    let mut out_offsets_len = 0;
    compress_bulk(
      &self.symbol_table,
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

impl Default for FsstEncoder {
  fn default() -> Self {
    Self::new()
  }
}
