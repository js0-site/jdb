use core::ops::Bound;
use std::borrow::Borrow;

use crate::pos::Pos;

pub trait Mem {
  type Key<'a>
  where
    Self: 'a;

  type Iter<'a>: Iterator<Item = (Self::Key<'a>, Pos)>
  where
    Self: 'a;

  type RevIter<'a>: Iterator<Item = (Self::Key<'a>, Pos)>
  where
    Self: 'a;

  fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos);

  fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos>;

  fn iter(&self) -> Self::Iter<'_>;

  fn rev_iter(&self) -> Self::RevIter<'_>;

  fn range<Start: Borrow<[u8]>, End: Borrow<[u8]>>(
    &self,
    start: Bound<Start>,
    end: Bound<End>,
  ) -> Self::Iter<'_>;

  fn rev_range<Start: Borrow<[u8]>, End: Borrow<[u8]>>(
    &self,
    end: Bound<End>,
    start: Bound<Start>,
  ) -> Self::RevIter<'_>;
}
