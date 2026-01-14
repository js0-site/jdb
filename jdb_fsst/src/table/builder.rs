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

  // Helper closure to match reference implementation behavior
  let add_or_inc = |cands: &mut HashSet<QSymbol>, s: Symbol, count: u64| {
      if count < (5 * frac as u64) / 128 {
          return;
      }
      let gain = (count * s.symbol_len() as u64) as u32;
      let q = QSymbol { symbol: s, gain };
      if let Some(old) = cands.take(&q) {
          cands.insert(QSymbol {
              symbol: s,
              gain: gain + old.gain,
          });
      } else {
          cands.insert(q);
      }
  };

  for pos1 in 0..CODE_BASE as usize + st.n_symbols as usize {
    let cnt1 = counters.count1_get(pos1);
    if cnt1 == 0 {
      continue;
    }
    let s1 = st.symbols[pos1];
    
    // Add current symbol
    add_or_inc(
        &mut candidates,
        s1,
        if s1.symbol_len() == 1 { 8 } else { 1 } * cnt1 as u64,
    );

    if s1.first() == st.terminator as u8 {
      continue;
    }

    if frac < 128 && s1.symbol_len() < MAX_SYMBOL_LEN as u32 {
      for pos2 in 0..CODE_BASE as usize + st.n_symbols as usize {
        let cnt2 = counters.count2_get(pos1, pos2);
        if cnt2 == 0 {
          continue;
        }
        let s2 = st.symbols[pos2];
        if s2.first() != st.terminator as u8 {
          let s3 = Symbol::concat(s1, s2);
          add_or_inc(&mut candidates, s3, cnt2 as u64);
        }
      }
    }
  }

  let mut pq: BinaryHeap<QSymbol> = BinaryHeap::new();
  for q in candidates {
    pq.push(q);
  }

  let terminator = st.terminator;
  *st = Table::new();
  st.terminator = terminator;
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

  let mut best_table = st.clone();
  let mut best_gain = 0isize - SAMPLEMAXSZ as isize;

  for frac in [8, 38, 68, 98, 108, 128] {
    let (mut counters, gain) = compute_gain_and_freqs(&mut st, &buf, &offsets, frac);
    if gain >= best_gain {
        best_gain = gain;
        best_table = st.clone();
    }
    update_symbol_table(&mut st, &mut counters, frac);
  }

  if best_table.n_symbols == 0 {
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      "failed to build symbol table",
    ));
  }

  Ok(best_table.finalize())
}
