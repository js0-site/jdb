//! Two-source merge fast path
//! 两源归并快速路径

use std::{cmp::Ordering, ops::ControlFlow};

use jdb_base::{Pos, order::Order};

use super::MergeIter;

impl<'a, I, O: Order> MergeIter<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  /// Two-source fast path (Break = return, Continue = loop)
  /// 两源快速路径 (Break = 返回, Continue = 继续循环)
  #[inline(always)]
  pub(crate) fn merge2(&mut self) -> ControlFlow<Option<(&'a [u8], Pos)>> {
    // Safety: Caller ensures len == 2, so indices 0 and 1 are populated (Some).
    // 安全性：调用者确保 len == 2，因此索引 0 和 1 已填充（Some）。
    let (n0, n1) = unsafe {
      (
        self
          .sources
          .get_unchecked(0)
          .as_ref()
          .unwrap_unchecked()
          .next,
        self
          .sources
          .get_unchecked(1)
          .as_ref()
          .unwrap_unchecked()
          .next,
      )
    };

    match (n0, n1) {
      (Some((k0, _)), Some((k1, _))) => {
        // Safe to create mutable references here as we won't be pruning
        // 此处创建可变引用是安全的，因为不会进行修剪
        // Safety: Distinct indices 0 and 1.
        let (s0, s1) = unsafe {
          let ptr = self.sources.as_mut_ptr();
          // (*ptr) is Option<Source>. We need &mut Option<Source> -> .as_mut() -> Option<&mut Source> -> unwrap
          (
            (&mut *ptr).as_mut().unwrap_unchecked(),
            (&mut *ptr.add(1)).as_mut().unwrap_unchecked(),
          )
        };
        ControlFlow::Break(match O::cmp(k0, k1) {
          Ordering::Less => s0.pop(),
          Ordering::Greater => s1.pop(),
          Ordering::Equal => {
            let _ = s1.pop();
            s0.pop()
          }
        })
      }
      (Some(_), None) => {
        self.prune(1);
        ControlFlow::Continue(())
      }
      (None, Some(_)) => {
        self.prune(0);
        ControlFlow::Continue(())
      }
      (None, None) => {
        // Both exhausted, clear everything
        self.len = 0;
        self.sources[0] = None;
        self.sources[1] = None;
        ControlFlow::Break(None)
      }
    }
  }
}
