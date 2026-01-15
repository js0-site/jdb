use std::io::{self, Write};

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

  /// Encode a single byte slice.
  /// 编码单个字节切片。
  pub fn encode(&self, bin: impl AsRef<[u8]>, mut out: impl Write) -> io::Result<usize> {
    let data = bin.as_ref();
    if data.is_empty() {
      return Ok(0);
    }

    if self.table.n_symbols == 0 {
      return out.write(data);
    }

    let mut buf = [0u8; 520];
    let mut written = 0;
    let mut in_curr = 0;
    let end_curr = data.len();

    while in_curr < end_curr {
      let this_len = (end_curr - in_curr).min(511);
      buf[..this_len].copy_from_slice(&data[in_curr..in_curr + this_len]);
      buf[this_len] = self.table.terminator as u8;

      let mut b_curr = 0;
      let mut out_buf = [0u8; 1024];
      let mut out_curr = 0;

      while b_curr < this_len {
        let word = unsafe { unaligned_load_unchecked(buf[b_curr..].as_ptr()) };
        let short_code = unsafe {
          *self
            .table
            .short_codes
            .get_unchecked((word & 0xFFFF) as usize)
        };
        let idx = crate::symbol::hash(word & 0xFFFFFF) as usize & (HASH_TAB_SIZE - 1);
        let s = unsafe { *self.table.hash_tab.get_unchecked(idx) };

        out_buf[out_curr + 1] = word as u8;

        let code = if s.icl < ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
          (s.icl >> 16) as u16
        } else {
          short_code
        };

        out_buf[out_curr] = code as u8;
        b_curr += (code >> CODE_LEN_SHIFT_IN_CODE) as usize;
        out_curr += 1 + ((code & CODE_BASE) >> 8) as usize;
      }

      out.write_all(&out_buf[..out_curr])?;
      written += out_curr;
      in_curr += this_len;
    }

    Ok(written)
  }
}
