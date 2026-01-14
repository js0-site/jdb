use std::{
  collections::{BinaryHeap, HashSet},
  io,
};

use crate::{
  CODE_BASE, MAX_SYMBOL_LEN, SAMPLEMAXSZ, SAMPLETARGET,
  counter::Counters,
  symbol::{QSymbol, Symbol},
  table::Table,
};

pub fn make_sample(in_buf: &[u8], offsets: &[usize]) -> (Vec<u8>, Vec<usize>) {
  if in_buf.len() <= SAMPLETARGET {
    return (in_buf.to_vec(), offsets.to_vec());
  }
  let mut sample_buf = Vec::with_capacity(SAMPLEMAXSZ);
  let mut sample_offsets = vec![0usize];
  while sample_buf.len() < SAMPLETARGET {
    let idx = fastrand::usize(0..offsets.len() - 1);
    let (start, end) = (offsets[idx], offsets[idx + 1]);
    sample_buf.extend_from_slice(&in_buf[start..end]);
    sample_offsets.push(sample_buf.len());
  }
  sample_offsets.push(sample_buf.len());
  (sample_buf, sample_offsets)
}

fn compute_gain_and_freqs(
  st: &mut Table,
  buf: &[u8],
  offsets: &[usize],
  frac: usize,
) -> (Counters, isize) {
  let mut gain: isize = 0;
  let mut counters = Counters::new();
  for win in offsets.windows(2) {
    let word = &buf[win[0]..win[1]];
    if word.is_empty() {
      continue;
    }
    let mut curr = 0;
    let mut prev_code = st.find_longest_symbol_from_char_slice(&word[curr..]);
    curr += st.symbols[prev_code as usize].symbol_len() as usize;
    gain += (st.symbols[prev_code as usize].symbol_len() as usize)
      .saturating_sub(1 + if prev_code < CODE_BASE { 1 } else { 0 }) as isize;

    while curr < word.len() {
      counters.count1_inc(prev_code);
      if st.symbols[prev_code as usize].symbol_len() != 1 {
        counters.count1_inc(word[curr] as u16);
      }
      let curr_code = st.find_longest_symbol_from_char_slice(&word[curr..]);
      let symbol_len = st.symbols[curr_code as usize].symbol_len();
      gain += (symbol_len as usize).saturating_sub(1 + if curr_code < CODE_BASE { 1 } else { 0 })
        as isize;
      if frac < 128 {
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
  (counters, gain)
}

fn update_symbol_table(st: &mut Table, counters: &mut Counters, frac: usize) {
  let mut candidates: HashSet<QSymbol> = HashSet::new();
  counters.count1_set(st.terminator as usize, u16::MAX);
  for pos1 in 0..CODE_BASE as usize + st.n_symbols as usize {
    let cnt1 = counters.count1_get(pos1);
    if cnt1 == 0 {
      continue;
    }
    let s1 = st.symbols[pos1];
    let gain =
      (if s1.symbol_len() == 1 { 8 } else { 1 } * cnt1 as u64 * s1.symbol_len() as u64) as u32;
    if gain >= (5 * frac as u64 / 128) as u32 {
      candidates.insert(QSymbol { symbol: s1, gain });
    }
    if frac < 128 && s1.symbol_len() < MAX_SYMBOL_LEN as u32 && s1.first() != st.terminator as u8
    {
      for pos2 in 0..CODE_BASE as usize + st.n_symbols as usize {
        let cnt2 = counters.count2_get(pos1, pos2);
        if cnt2 == 0 {
          continue;
        }
        let s2 = st.symbols[pos2];
        if s2.first() != st.terminator as u8 {
          let s3 = Symbol::concat(s1, s2);
          let g = (cnt2 as u64 * s3.symbol_len() as u64) as u32;
          if g >= (5 * frac as u64 / 128) as u32 {
            candidates.insert(QSymbol {
              symbol: s3,
              gain: g,
            });
          }
        }
      }
    }
  }
  let mut pq: BinaryHeap<QSymbol> = candidates.into_iter().collect();
  *st = Table::new();
  while st.n_symbols < 255 && !pq.is_empty() {
    st.add(pq.pop().unwrap().symbol);
  }
}

pub fn build_symbol_table(buf: Vec<u8>, offsets: Vec<usize>) -> io::Result<crate::encode::Encode> {
  let mut st = Table::new();
  let mut byte_histo = [0usize; 256];
  for &c in &buf {
    byte_histo[c as usize] += 1;
  }
  if let Some((idx, _)) = byte_histo.iter().enumerate().min_by_key(|&(_, cnt)| cnt) {
    st.terminator = idx as u16;
  }

  for frac in [8, 38, 68, 128] {
    let (mut counters, _) = compute_gain_and_freqs(&mut st, &buf, &offsets, frac);
    update_symbol_table(&mut st, &mut counters, frac);
  }
  
  if st.n_symbols == 0 {
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      "failed to build symbol table",
    ));
  }
  
  Ok(st.finalize())
}
