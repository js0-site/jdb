use crate::{
  error::Result,
  fsst::Fsst,
  table::builder,
};

/// Train FSST symbol table from a list of byte slices.
/// 从字节切片列表训练 FSST 符号表。
///
/// # Arguments
/// * `li` - A list of byte slices to train on.
///
/// # Returns
/// Returns a `Fsst` symbol table that can be used for encoding/decoding.
/// 返回可用于编码/解码的 `Fsst` 符号表。
pub fn train<T: AsRef<[u8]>>(li: &[T]) -> Result<Fsst> {
  if li.is_empty() {
    return Ok(Fsst::default());
  }

  // Build symbol table from the input items
  // 从输入项构建符号表
  let table = builder::build_symbol_table_from_items(li)?;

  // Convert Table to Fsst
  // 将 Table 转换为 Fsst
  let mut fsst = Fsst::new(table.n_symbols as u8);
  if fsst.n_symbols > 0 {
    for i in 0..table.n_symbols as usize {
      let s = unsafe { *table.symbols.get_unchecked(crate::CODE_BASE as usize + i) };
      fsst.symbol[i] = s.val;
      fsst.len[i] = s.symbol_len() as u8;
    }
  }

  Ok(fsst)
}