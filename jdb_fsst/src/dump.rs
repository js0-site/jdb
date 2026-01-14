use crate::{decode::Decode, encode::Encode, table::Table};

/// Convert a `Table` and encode state to a serializable snapshot.
/// 将 `Table` 和编码器状态转换为可序列化的快照。
///
/// This avoids the legacy "header + byte array" layout and produces a clean
/// struct that can be compressed with `bitcode`.
pub fn create_head(st: &Table, encode_switch: bool) -> Decode {
  if !encode_switch || st.n_symbols == 0 {
    return Decode::default();
  }

  Decode::from(Encode::from(st.clone()))
}

/// Serialize the symbol table to bytes using bitcode.
/// 使用 bitcode 将符号表序列化为字节。
pub fn to_bytes(st: &Table, encode_switch: bool) -> Vec<u8> {
  bitcode::encode(&create_head(st, encode_switch))
}
