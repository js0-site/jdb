use std::io;

use crate::{
  CODE_BASE, CODE_LEN_SHIFT_IN_CODE, HASH_TAB_SIZE, ICL_FREE, symbol::hash, table::Table,
  unaligned_load_unchecked,
};

pub fn batch(
  st: &Table,
  strs: &[u8],
  offsets: &[usize],
  out: &mut Vec<u8>,
  out_offsets: &mut Vec<usize>,
  out_pos: &mut usize,
  out_offsets_len: &mut usize,
) -> io::Result<()> {
  let mut out_curr = *out_pos;
  let mut buf = [0u8; 520];
  out_offsets[0] = *out_pos;

  for (i, win) in offsets.windows(2).enumerate() {
    let (mut in_curr, end_curr) = (win[0], win[1]);
    while in_curr < end_curr {
      let this_len = (end_curr - in_curr).min(511);
      buf[..this_len].copy_from_slice(&strs[in_curr..in_curr + this_len]);
      buf[this_len] = st.terminator as u8;

      let mut b_curr = 0;
      while b_curr < this_len {
        let word = unsafe { unaligned_load_unchecked(buf[b_curr..].as_ptr()) };
        let short_code = unsafe { *st.short_codes.get_unchecked((word & 0xFFFF) as usize) };
        let idx = hash(word & 0xFFFFFF) as usize & (HASH_TAB_SIZE - 1);
        let s = unsafe { *st.hash_tab.get_unchecked(idx) };

        let code = if s.icl < ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF))) {
          (s.icl >> 16) as u16
        } else {
          short_code
        };

        // Write the code byte
        // 写入编码字节
        out[out_curr] = code as u8;
        out_curr += 1;

        // If it's an escape code (check 9th bit), write the literal byte
        if (code & CODE_BASE) != 0 {
          out[out_curr] = word as u8;
          out_curr += 1;
        }

        b_curr += (code >> CODE_LEN_SHIFT_IN_CODE) as usize;
      }
      in_curr += this_len;
    }
    out_offsets[i + 1] = out_curr;
  }
  out.truncate(out_curr);
  out_offsets.truncate(offsets.len());
  *out_pos = out_curr;
  *out_offsets_len = offsets.len();
  Ok(())
}
