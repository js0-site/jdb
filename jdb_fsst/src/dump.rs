use crate::{dict::Dict, table::Table};

/// Convert a `Table` and encode state to a serializable snapshot.
/// 将 `Table` 和编码器状态转换为可序列化的快照。
///
/// This avoids the legacy "header + byte array" layout and produces a clean
/// struct that can be compressed with `bitcode`.
pub fn create_head(st: &Table, encode_switch: bool) -> Dict {
  if !encode_switch || st.n_symbols == 0 {
    return Dict::default();
  }

  let mut len = [0u8; 256];
  let mut symbol = [0u64; 256];
  let n = st.n_symbols as usize;

  for i in 0..n {
    // SAFETY: n_symbols limits access to valid range
    // 安全性：n_symbols 限制访问有效范围
    let s = st.symbols[crate::CODE_BASE as usize + i];
    symbol[i] = s.val;
    len[i] = s.symbol_len() as u8;
  }

  Dict {
    n_symbols: n as u8,
    len,
    symbol,
  }
}

/// Serialize the symbol table to bytes using bitcode.
/// 使用 bitcode 将符号表序列化为字节。
pub fn to_bytes(st: &Table, encode_switch: bool) -> Vec<u8> {
  let head = create_head(st, encode_switch);
  bitcode::encode(&head)
}
