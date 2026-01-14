use std::{
  cmp::Ordering,
  fmt,
  hash::{Hash, Hasher},
};

use crate::{CODE_LEN_SHIFT_IN_ICL, CODE_MASK, CODE_SHIFT_IN_ICL, ICL_FREE, MAX_SYMBOL_LENGTH};

#[inline]
pub fn hash(w: u64) -> u64 {
  wyhash::wyhash(&w.to_le_bytes(), 0)
}

#[derive(Default, Copy, Clone, PartialEq, Eq)]
pub struct Symbol {
  // the byte sequence that this symbol stands for
  pub val: u64,

  // icl = u64 ignoredBits:16,code:12,length:4,unused:32 -- but we avoid exposing this bit-field notation
  // use a single u64 to be sure "code" is accessed with one load and can be compared with one comparison
  pub icl: u64,
}

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
  pub fn new() -> Self {
    Self {
      val: 0,
      icl: ICL_FREE,
    }
  }

  pub fn from_char(c: u8, code: u16) -> Self {
    Self {
      val: c as u64,
      // in a symbol which represents a single character, 56 bits(7 bytes) are ignored, code length is 1
      icl: (1 << CODE_LEN_SHIFT_IN_ICL) | ((code as u64) << CODE_SHIFT_IN_ICL) | 56,
    }
  }

  pub fn set_code_len(&mut self, code: u16, len: u32) {
    self.icl = ((len as u64) << CODE_LEN_SHIFT_IN_ICL)
      | ((code as u64) << CODE_SHIFT_IN_ICL)
      | ((8u64.saturating_sub(len as u64)) * 8);
  }

  #[inline]
  pub fn symbol_len(&self) -> u32 {
    (self.icl >> CODE_LEN_SHIFT_IN_ICL) as u32
  }

  #[inline]
  pub fn code(&self) -> u16 {
    ((self.icl >> CODE_SHIFT_IN_ICL) & CODE_MASK as u64) as u16
  }

  // ignoredBits is (8-length)*8, which is the amount of high bits to zero in the input word before comparing with the hashtable key
  // it could of course be computed from len during lookup, but storing it precomputed in some loose bits is faster
  #[inline]
  pub fn ignored_bits(&self) -> u32 {
    (self.icl & u16::MAX as u64) as u32
  }

  #[inline]
  pub fn first(&self) -> u8 {
    assert!(self.symbol_len() >= 1);
    (0xFF & self.val) as u8
  }

  #[inline]
  pub fn first2(&self) -> u16 {
    assert!(self.symbol_len() >= 2);
    (0xFFFF & self.val) as u16
  }

  #[inline]
  pub fn hash(&self) -> u64 {
    let v = 0xFFFFFF & self.val;
    hash(v)
  }

  // right is the substring follows left
  // for example, in "hello",
  // "llo" is the substring that follows "he"
  pub fn concat(left: Self, right: Self) -> Self {
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
pub struct QSymbol {
  pub symbol: Symbol,
  // the gain field is only used in the symbol queue that sorts symbols on gain
  pub gain: u32,
}

impl PartialEq for QSymbol {
  fn eq(&self, other: &Self) -> bool {
    self.symbol.val == other.symbol.val && self.symbol.icl == other.symbol.icl
  }
}

impl Ord for QSymbol {
  fn cmp(&self, other: &Self) -> Ordering {
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
