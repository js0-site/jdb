//! Two-source merge fast path
//! 两源归并快速路径

use std::ops::ControlFlow;

use jdb_base::{Pos, order::Order};

use super::{Merge, MergeIter, Two};

impl Merge for Two {
  #[inline(always)]
  fn merge<'a, I, O: Order, const N: usize>(
    iter: &mut MergeIter<'a, I, O, N, Self>,
  ) -> Option<(&'a [u8], Pos)>
  where
    I: Iterator<Item = (&'a [u8], Pos)>,
  {
    loop {
      match iter.len {
        0 => return None,
        1 => {
          if let ControlFlow::Break(v) = unsafe { iter.merge1_step() } {
            return v;
          }
        }
        _ => {
          if let ControlFlow::Break(v) = unsafe { iter.merge2_step() } {
            return v;
          }
        }
      }
    }
  }
}
