use bitcode::{Decode, Encode};

/// The on-disk serialization format for the FSST symbol table (Dictionary).
/// FSST 符号表（字典）的磁盘序列化格式。
#[derive(Debug, Clone, PartialEq, Eq, Copy, Encode, Decode)]
pub struct Dict {
  /// Number of symbols in the table (0 means FSST is disabled).
  /// 符号表中的符号数量（0 表示未启用 FSST）。
  pub n_symbols: u8,

  /// Symbol lengths (values are in range 1-8).
  /// 符号长度。
  pub len: [u8; 256],

  /// Symbol values (bits 56-63 are ignored/garbage).
  /// 符号值。
  pub symbol: [u64; 256],
}

impl Default for Dict {
  fn default() -> Self {
    Self::new(0)
  }
}

impl Dict {
  pub const fn new(n_symbols: u8) -> Self {
    Self {
      n_symbols,
      len: [0; 256],
      symbol: [0; 256],
    }
  }
  /// Check if the dictionary is enabled.
  pub fn is_enabled(&self) -> bool {
    self.n_symbols > 0
  }
}
