//! Three-source merge fast path
//! 三源归并快速路径

use std::{cmp::Ordering, ops::ControlFlow};

use jdb_base::{Pos, order::Order};

use super::{Merge, MergeIter, Three};

impl Merge for Three {
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
        2 => {
          if let ControlFlow::Break(v) = unsafe { iter.merge2_step() } {
            return v;
          }
        }
        _ => {
          // Safety: len >= 3 guarantees sources[0, 1, 2] are Some.
          let ptr = iter.sources.as_mut_ptr();
          let s0 = unsafe { (&mut *ptr).as_mut().unwrap_unchecked() };
          if s0.next.is_none() {
            iter.prune(0);
            continue;
          }

          let s1 = unsafe { (&mut *ptr.add(1)).as_mut().unwrap_unchecked() };
          if s1.next.is_none() {
            iter.prune(1);
            continue;
          }

          let s2 = unsafe { (&mut *ptr.add(2)).as_mut().unwrap_unchecked() };
          if s2.next.is_none() {
            iter.prune(2);
            continue;
          }

          let (k0, k1, k2) = unsafe {
            (
              s0.next.unwrap_unchecked().0,
              s1.next.unwrap_unchecked().0,
              s2.next.unwrap_unchecked().0,
            )
          };

          // Find best key among 3 sources
          // 在 3 个源中找到最佳键
          let (best01, best_key01, _other01) = match O::cmp(k0, k1) {
            Ordering::Less => (s0, k0, Some(s1)),
            Ordering::Greater => (s1, k1, Some(s0)),
            Ordering::Equal => {
              let _ = s1.pop(); // dedup
              (s0, k0, None)
            }
          };

          match O::cmp(best_key01, k2) {
            Ordering::Less => return best01.pop(),
            Ordering::Greater => return s2.pop(),
            Ordering::Equal => {
              let _ = s2.pop(); // dedup
              return best01.pop();
            }
          }
        }
      }
    }
  }
}
