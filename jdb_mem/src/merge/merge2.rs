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
    let ptr = self.sources.as_mut_ptr();

    // Read next items safely without creating overlapping mutable references
    // 安全读取下一个条目，避免创建重叠的可变引用
    let (n0, n1) = unsafe { ((*ptr).next, (*ptr.add(1)).next) };

    match (n0, n1) {
      (Some((k0, _)), Some((k1, _))) => {
        // Safe to create mutable references here as we won't be pruning
        // 此处创建可变引用是安全的，因为不会进行修剪
        let (s0, s1) = unsafe { (&mut *ptr, &mut *ptr.add(1)) };
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
        self.sources.clear();
        ControlFlow::Break(None)
      }
    }
  }
}
