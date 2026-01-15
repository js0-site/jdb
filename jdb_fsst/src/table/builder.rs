use std::{collections::BinaryHeap, io};

use gxhash::HashMap as GxHashMap;

use crate::{
  CODE_BASE, MAX_SYMBOL_LEN, SAMPLEMAXSZ, SAMPLETARGET,
  counter::Counters,
  symbol::{QSymbol, Symbol},
  table::Table,
};

/// Build symbol table from list of items with built-in sampling.
/// 从项目列表构建符号表，内置采样。
pub fn build_from_items<T: AsRef<[u8]>>(li: &[T]) -> io::Result<crate::encode::Encode> {
  if li.is_empty() {
    return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty input"));
  }

  // Calculate total size and sample if needed
  // 计算总大小并在需要时采样
  let total_size: usize = li.iter().map(|item| item.as_ref().len()).sum();

  let (sample_buf, sample_offsets) = if total_size <= SAMPLETARGET {
    // Use all data if small enough
    // 如果足够小则使用所有数据
    let mut buf = Vec::with_capacity(total_size);
    let mut offsets = vec![0];
    for item in li {
      buf.extend_from_slice(item.as_ref());
      offsets.push(buf.len());
    }
    (buf, offsets)
  } else {
    // Random sampling
    // 随机采样
    let mut buf = Vec::with_capacity(SAMPLEMAXSZ);
    let mut offsets = vec![0];
    while buf.len() < SAMPLETARGET && !li.is_empty() {
      let idx = fastrand::usize(0..li.len());
      let item = li[idx].as_ref();
      buf.extend_from_slice(item);
      offsets.push(buf.len());
    }
    (buf, offsets)
  };

  build_symbol_table(sample_buf, sample_offsets)
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
  let mut candidates: GxHashMap<u64, QSymbol> = GxHashMap::default();
  counters.count1_set(st.terminator as usize, u16::MAX);

  let threshold = (5 * frac as u64) / 128;

  // Helper to add or increment candidate gain
  // 添加或增加候选增益
  let mut add_or_inc = |s: Symbol, count: u64| {
    if count < threshold {
      return;
    }
    let gain = (count * s.symbol_len() as u64) as u32;
    candidates
      .entry(s.val)
      .and_modify(|q| q.gain += gain)
      .or_insert(QSymbol { symbol: s, gain });
  };

  for pos1 in 0..CODE_BASE as usize + st.n_symbols as usize {
    let cnt1 = counters.count1_get(pos1);
    if cnt1 == 0 {
      continue;
    }
    let s1 = st.symbols[pos1];

    // Add current symbol
    // 添加当前符号
    add_or_inc(s1, if s1.symbol_len() == 1 { 8 } else { 1 } * cnt1 as u64);

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
          add_or_inc(s3, cnt2 as u64);
        }
      }
    }
  }

  let mut pq: BinaryHeap<QSymbol> = candidates.into_values().collect();

  let terminator = st.terminator;
  *st = Table::new();
  st.terminator = terminator;
  while st.n_symbols < 255 {
    if let Some(q) = pq.pop() {
      st.add(q.symbol);
    } else {
      break;
    }
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
