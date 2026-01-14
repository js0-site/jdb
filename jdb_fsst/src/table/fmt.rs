use std::fmt;

use super::Table;
use crate::{CODE_BASE, CODE_BITS};

impl fmt::Display for Table {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f, "A FSST Table after finalize():")?;
    writeln!(f, "n_symbols: {}", self.n_symbols)?;
    for i in 0_usize..self.n_symbols as usize {
      writeln!(f, "symbols[{}]: {}", i, self.symbols[i])?;
    }
    writeln!(f, "suffix_lim: {}", self.suffix_lim)?;
    for i in 0..CODE_BITS {
      writeln!(f, "len_histo[{}]: {}", i, self.len_histo[i as usize])?;
    }
    Ok(())
  }
}

impl fmt::Debug for Table {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f, "A FSST Table before finalize():")?;
    writeln!(f, "n_symbols: {}", self.n_symbols)?;
    for i in CODE_BASE as usize..CODE_BASE as usize + self.n_symbols as usize {
      writeln!(f, "symbols[{}]: {}", i, self.symbols[i])?;
    }
    writeln!(f, "suffix_lim: {}", self.suffix_lim)?;
    for i in 0..CODE_BITS {
      writeln!(f, "len_histo[{}]: {}\n", i, self.len_histo[i as usize])?;
    }
    Ok(())
  }
}
