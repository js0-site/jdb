//! N-way general merge
//! N 路通用归并

use std::{cmp::Ordering, ops::ControlFlow};

use jdb_base::{Pos, order::Order};

use super::MergeIter;

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// N-way general merge (Break = return, Continue = loop)
  /// N 路通用归并 (Break = 返回, Continue = 继续循环)
  #[inline]
  pub(crate) fn merge_n(&mut self) -> ControlFlow<Option<(&'a [u8], Pos)>> {
    let len = self.sources.len();
    let mut best_idx = 0;
    let mut best_key = match unsafe { self.sources.get_unchecked(0) }.next {
      Some((k, _)) => k,
      None => {
        self.prune(0);
        return ControlFlow::Continue(());
      }
    };

    for i in 1..len {
      let source = unsafe { self.sources.get_unchecked_mut(i) };
      if let Some((key, _)) = source.next {
        let cmp = O::cmp(key, best_key);
        if cmp == Ordering::Less {
          best_idx = i;
          best_key = key;
        } else if cmp == Ordering::Equal {
          let _ = source.pop();
        }
      } else {
        self.prune(i);
        return ControlFlow::Continue(());
      }
    }

    ControlFlow::Break(unsafe { self.sources.get_unchecked_mut(best_idx).pop() })
  }
}
