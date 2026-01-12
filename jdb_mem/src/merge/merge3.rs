//! Three-source merge fast path
//! 三源归并快速路径

use std::{cmp::Ordering, ops::ControlFlow};

use jdb_base::{Pos, order::Order};

use super::{MergeIter, Source};

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Three-source fast path (Break = return, Continue = loop)
  /// 三源快速路径 (Break = 返回, Continue = 继续循环)
  #[inline(always)]
  pub(crate) fn merge3(&mut self) -> ControlFlow<Option<(&'a [u8], Pos)>> {
    // Safety: Caller ensures len == 3, so indices 0, 1, 2 are populated (Some).
    // 安全性：调用者确保 len == 3，因此索引 0、1、2 已填充（Some）。
    let (n0, n1, n2) = unsafe {
      (
        Option::as_ref(self.sources.get_unchecked(0))
          .unwrap_unchecked()
          .next,
        Option::as_ref(self.sources.get_unchecked(1))
          .unwrap_unchecked()
          .next,
        Option::as_ref(self.sources.get_unchecked(2))
          .unwrap_unchecked()
          .next,
      )
    };

    match (n0, n1, n2) {
      (Some((k0, _)), Some((k1, _)), Some((k2, _))) => {
        // Find best key among 3 sources
        // 在 3 个源中找到最佳键
        // Safety: Distinct indices 0, 1, 2.
        let (s0, s1, s2) = unsafe {
          let ptr = self.sources.as_mut_ptr();
          (
            Option::as_mut(&mut *ptr).unwrap_unchecked(),
            Option::as_mut(&mut *ptr.add(1)).unwrap_unchecked(),
            Option::as_mut(&mut *ptr.add(2)).unwrap_unchecked(),
          )
        };

        // Compare k0 vs k1
        let (best01, best_key01, _other01) = match O::cmp(k0, k1) {
          Ordering::Less => (s0, k0, Some::<&mut Source<'a, I>>(s1)),
          Ordering::Greater => (s1, k1, Some::<&mut Source<'a, I>>(s0)),
          Ordering::Equal => {
            let _ = s1.pop(); // dedup
            (s0, k0, None)
          }
        };

        // Compare winner with k2
        match O::cmp(best_key01, k2) {
          Ordering::Less => ControlFlow::Break(best01.pop()),
          Ordering::Greater => ControlFlow::Break(s2.pop()),
          Ordering::Equal => {
            let _ = s2.pop(); // dedup
            ControlFlow::Break(best01.pop())
          }
        }
      }
      // One or more sources exhausted, prune and continue
      // 一个或多个源已耗尽，修剪并继续
      (None, ..) => {
        self.prune(0);
        ControlFlow::Continue(())
      }
      (_, None, _) => {
        self.prune(1);
        ControlFlow::Continue(())
      }
      (_, _, None) => {
        self.prune(2);
        ControlFlow::Continue(())
      }
    }
  }
}
