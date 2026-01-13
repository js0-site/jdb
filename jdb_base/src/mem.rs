use core::ops::RangeBounds;
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

  type Error: std::fmt::Debug + Send;

  fn put(
    &mut self,
    key: impl Into<Box<[u8]>>,
    pos: Pos,
  ) -> impl std::future::Future<Output = Result<(), Self::Error>>;

  fn get(&self, key: impl Borrow<[u8]>) -> Option<Pos>;

  fn sync(&mut self) -> impl std::future::Future<Output = Result<(), Self::Error>>;

  fn iter(&self) -> Self::Iter<'_>;

  fn rev_iter(&self) -> Self::RevIter<'_>;

  fn range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::Iter<'_>;

  fn rev_range<Q: ?Sized + Borrow<[u8]>>(&self, range: impl RangeBounds<Q>) -> Self::RevIter<'_>;
}
