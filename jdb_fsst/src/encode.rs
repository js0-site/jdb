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

    let in_end = data.len();
    let in_ptr = data.as_ptr();

    unsafe {
      out.set_len(start + needed);
      let out_base = out.as_mut_ptr().add(start);
      let mut in_curr = 0;
      let mut out_curr = 0;

      // Process with 8-byte lookahead safety margin
      // 使用 8 字节前瞻安全边界处理
      while in_curr + 8 <= in_end {
        let word = unaligned_load_unchecked(in_ptr.add(in_curr));
        let short_code = *self
          .table
          .short_codes
          .get_unchecked((word & 0xFFFF) as usize);
        let idx = crate::symbol::hash(word & 0xFFFFFF) as usize & (HASH_TAB_SIZE - 1);
        let s = *self.table.hash_tab.get_unchecked(idx);

        // Speculatively write escaped byte
        // 推测性写入转义字节
        *out_base.add(out_curr + 1) = word as u8;

        let code = if s.icl < ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
          (s.icl >> 16) as u16
        } else {
          short_code
        };

        *out_base.add(out_curr) = code as u8;
        in_curr += (code >> CODE_LEN_SHIFT_IN_CODE) as usize;
        out_curr += 1 + ((code & CODE_BASE) >> 8) as usize;
      }

      // Handle remaining bytes with terminator padding
      // 处理剩余字节，使用终止符填充
      if in_curr < in_end {
        let remain = in_end - in_curr;
        let mut buf = [self.table.terminator as u8; 16];
        std::ptr::copy_nonoverlapping(in_ptr.add(in_curr), buf.as_mut_ptr(), remain);

        let mut b_curr = 0;
        while b_curr < remain {
          let word = unaligned_load_unchecked(buf.as_ptr().add(b_curr));
          let short_code = *self
            .table
            .short_codes
            .get_unchecked((word & 0xFFFF) as usize);
          let idx = crate::symbol::hash(word & 0xFFFFFF) as usize & (HASH_TAB_SIZE - 1);
          let s = *self.table.hash_tab.get_unchecked(idx);

          *out_base.add(out_curr + 1) = word as u8;

          let code = if s.icl < ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
            (s.icl >> 16) as u16
          } else {
            short_code
          };

          *out_base.add(out_curr) = code as u8;
          b_curr += (code >> CODE_LEN_SHIFT_IN_CODE) as usize;
          out_curr += 1 + ((code & CODE_BASE) >> 8) as usize;
        }
      }

      out.set_len(start + out_curr);
      out_curr
    }
  }
}
