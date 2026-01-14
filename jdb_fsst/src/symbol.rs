use std::{
  cmp::Ordering,
  fmt,
  hash::{Hash, Hasher},
};

use crate::{CODE_LEN_SHIFT_IN_ICL, CODE_MASK, CODE_SHIFT_IN_ICL, ICL_FREE, MAX_SYMBOL_LEN};

/// FSST hash function optimized for 3-byte prefixes.
/// FSST 哈希函数，针对 3 字节前缀优化。
#[inline(always)]
pub fn hash(w: u64) -> u64 {
  const HASH_PRIME: u64 = 2971215073;
  let h = w.wrapping_mul(HASH_PRIME);
  h ^ (h >> 15)
}

#[derive(Default, Copy, Clone, PartialEq, Eq)]
pub struct Symbol {
  // the byte sequence that this symbol stands for
  // 此符号代表的字节序列
  pub val: u64,

  // icl = u64 ignoredBits:16,code:12,length:4,unused:32 -- but we avoid exposing this bit-field notation
  // use a single u64 to be sure "code" is accessed with one load and can be compared with one comparison
  // icl包含 ignoredBits:16, code:12, length:4, unused:32。避免位域表示，使用单个 u64 确保原子访问和比较
  pub icl: u64,
}

impl fmt::Display for Symbol {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let bytes = self.val.to_ne_bytes();
    // SAFETY: symbol_len() is always <= 8
    // 安全性：symbol_len() 总是 <= 8
    for i in 0..self.symbol_len() as usize {
      // SAFETY: i is within bounds 0..8
      let c = unsafe { *bytes.get_unchecked(i) };
      write!(f, "{}", c as char)?;
    }
    write!(
      f,
      "\tignored: {}, code: {}, len: {}",
      self.ignored_bits(),
      self.code(),
      self.symbol_len()
    )
  }
}

impl fmt::Debug for Symbol {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(self, f)
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
      // 单字符符号，忽略 56 位（7 字节），代码长度为 1
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
  // ignoredBits 是 (8-length)*8，即比较哈希表键之前需要置零的高位数量。预计算存储比查表时计算更快
  #[inline]
  pub fn ignored_bits(&self) -> u32 {
    (self.icl & u16::MAX as u64) as u32
  }

  // Returns the first byte of the symbol
  // 返回符号的第一个字节
  #[inline]
  pub fn first(&self) -> u8 {
    debug_assert!(self.symbol_len() >= 1);
    (self.val & 0xFF) as u8
  }

  // Returns the first two bytes of the symbol
  // 返回符号的前两个字节
  #[inline]
  pub fn first2(&self) -> u16 {
    debug_assert!(self.symbol_len() >= 2);
    (self.val & 0xFFFF) as u16
  }

  #[inline]
  pub fn hash(&self) -> u64 {
    let v = 0xFFFFFF & self.val;
    hash(v)
  }

  // right is the substring follows left
  // for example, in "hello",
  // "llo" is the substring that follows "he"
  // right 是紧随 left 之后的子串。例如在 "hello" 中，"llo" 是 "he" 的后继
  pub fn concat(left: Self, right: Self) -> Self {
    let mut s = Self::new();
    let mut length = left.symbol_len() + right.symbol_len();
    if length > MAX_SYMBOL_LEN as u32 {
      length = MAX_SYMBOL_LEN as u32;
    }
    s.set_code_len(CODE_MASK, length);
    s.val = (right.val << (8 * left.symbol_len())) | left.val;
    s
  }
}

// Symbol that can be put in a queue, ordered on gain
// 可放入队列的符号，按增益排序
pub struct QSymbol {
  pub symbol: Symbol,
  // the gain field is only used in the symbol queue that sorts symbols on gain
  // gain 字段仅用于按增益排序的符号队列
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
  // Use wyhash for efficient hashing; HashSet will rehash the output anyway.
  // 使用 wyhash 进行高效哈希；HashSet 会对输出再次哈希。
  fn hash<H: Hasher>(&self, state: &mut H) {
    // Hash the symbol value using wyhash, similar to Symbol::hash()
    // 使用 wyhash 哈希符号值，与 Symbol::hash() 类似
    let h = hash(self.symbol.val);
    h.hash(state);
  }
}
