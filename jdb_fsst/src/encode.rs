use std::io;

use crate::{
  LEAST_INPUT_SIZE,
  dict::Dict,
  table::{Table, batch, builder},
};

/// Finalized FSST encoder with optimized symbol table layout.
/// 最终化的 FSST 编码器，具有优化的符号表布局。
///
/// This is a finalized (frozen) Table that's ready for encoding.
/// The symbol table has been optimized and can no longer be modified.
/// 这是一个最终化（冻结）的 Table，已准备好进行编码。
/// 符号表已被优化，无法再修改。
pub struct Encode {
  /// The finalized, immutable symbol table.
  /// 最终化的、不可变的符号表。
  table: Table,
}

impl From<Table> for Encode {
  fn from(table: Table) -> Self {
    Self { table }
  }
}

impl Encode {
  /// Convert the encoder to a serializable dictionary.
  /// 将编码器转换为可序列化的字典。
  pub fn to_dict(&self) -> Dict {
    let mut dict = Dict::new(self.table.n_symbols as u8);
    if dict.n_symbols > 0 {
      for i in 0..self.table.n_symbols as usize {
        let s = unsafe { *self.table.symbols.get_unchecked(i) };
        dict.symbol[i] = s.val;
        dict.len[i] = s.symbol_len() as u8;
      }
    }
    dict
  }

  /// Encode input data using the symbol table.
  /// 使用符号表编码输入数据。
  pub fn encode(
    &self,
    in_buf: &[u8],
    in_offsets: &[usize],
    out_buf: &mut Vec<u8>,
    out_offsets: &mut Vec<usize>,
  ) -> io::Result<Dict> {
    let dict = self.to_dict();
    if dict.n_symbols == 0 {
      out_buf.resize(in_buf.len(), 0);
      out_buf.copy_from_slice(in_buf);
      out_offsets.resize(in_offsets.len(), 0);
      out_offsets.copy_from_slice(in_offsets);
      return Ok(dict);
    }
    let mut out_pos = 0;
    let mut out_offsets_len = 0;
    batch(
      &self.table,
      in_buf,
      in_offsets,
      out_buf,
      out_offsets,
      &mut out_pos,
      &mut out_offsets_len,
    )?;
    Ok(dict)
  }
}

/// Build symbol table and encode input data in one step.
/// 一步完成符号表构建和数据编码。
///
/// If the input is too small (< LEAST_INPUT_SIZE), returns empty dict and copies the input.
/// 如果输入太小（< LEAST_INPUT_SIZE），返回空字典并复制输入。
pub fn build_and_encode(
  in_buf: &[u8],
  in_offsets: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets: &mut Vec<usize>,
) -> io::Result<Dict> {
  // Ensure output buffers have enough capacity
  // 确保输出缓冲区有足够的容量
  out_buf.resize(in_buf.len() * 2, 0);
  out_offsets.resize(in_offsets.len(), 0);

  // If input is too small, skip compression
  // 如果输入太小，跳过压缩
  if in_buf.len() < LEAST_INPUT_SIZE {
    out_buf.resize(in_buf.len(), 0);
    out_buf.copy_from_slice(in_buf);
    out_offsets.copy_from_slice(in_offsets);
    return Ok(Dict::default());
  }

  // Build symbol table from sample
  // 从样本构建符号表
  let (sample, sample_offsets) = builder::make_sample(in_buf, in_offsets);
  let encoder = builder::build_symbol_table(sample, sample_offsets)?;

  // Encode using the built symbol table
  // 使用构建的符号表进行编码
  encoder.encode(in_buf, in_offsets, out_buf, out_offsets)
}
