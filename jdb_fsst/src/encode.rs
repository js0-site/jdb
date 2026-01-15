use crate::{
  CODE_BASE, CODE_LEN_SHIFT_IN_CODE, HASH_TAB_SIZE, ICL_FREE, symbol::Symbol, table::Table,
  unaligned_load_unchecked,
};

/// Finalized FSST encoder with optimized symbol table layout.
/// 最终化的 FSST 编码器，具有优化的符号表布局。
pub struct Encode {
  /// The finalized, immutable symbol table.
  /// 最终化的、不可变的符号表。
  pub table: Table,
}

impl From<Table> for Encode {
  fn from(table: Table) -> Self {
    Self { table }
  }
}

impl Encode {
  pub fn n_symbols(&self) -> usize {
    self.table.n_symbols as usize
  }

  pub fn symbol(&self, i: usize) -> Symbol {
    unsafe { *self.table.symbols.get_unchecked(i) }
  }

  /// Encode and append to Vec, return bytes written.
  /// 编码并追加到 Vec，返回写入字节数。
  #[inline]
  pub fn encode(&self, data: &[u8], out: &mut Vec<u8>) -> usize {
    if data.is_empty() {
      return 0;
    }

    if self.table.n_symbols == 0 {
      out.extend_from_slice(data);
      return data.len();
    }

    let start = out.len();
    let needed = data.len() * 2 + 16;
    out.reserve(needed);

    let mut buf = [0u8; 520];
    let mut in_curr = 0;
    let end_curr = data.len();
    let mut total_written = 0;

    unsafe {
      out.set_len(start + needed);
      let out_slice = &mut out[start..];

      while in_curr < end_curr {
        let this_len = (end_curr - in_curr).min(511);
        std::ptr::copy_nonoverlapping(data.as_ptr().add(in_curr), buf.as_mut_ptr(), this_len);
        buf[this_len] = self.table.terminator as u8;

        let mut b_curr = 0;
        let mut out_curr = total_written;

        while b_curr < this_len {
          let word = unaligned_load_unchecked(buf.as_ptr().add(b_curr));
          let short_code = *self.table.short_codes.get_unchecked((word & 0xFFFF) as usize);
          let idx = crate::symbol::hash(word & 0xFFFFFF) as usize & (HASH_TAB_SIZE - 1);
          let s = *self.table.hash_tab.get_unchecked(idx);

          // Speculatively write escaped byte
          // 推测性写入转义字节
          *out_slice.get_unchecked_mut(out_curr + 1) = word as u8;

          let code = if s.icl < ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
            (s.icl >> 16) as u16
          } else {
            short_code
          };

          *out_slice.get_unchecked_mut(out_curr) = code as u8;
          b_curr += (code >> CODE_LEN_SHIFT_IN_CODE) as usize;
          out_curr += 1 + ((code & CODE_BASE) >> 8) as usize;
        }

        total_written = out_curr;
        in_curr += this_len;
      }

      out.set_len(start + total_written);
    }

    total_written
  }
}
