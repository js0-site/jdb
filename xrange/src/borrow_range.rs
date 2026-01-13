use std::ops::{Bound, RangeBounds};

#[derive(Clone, Copy)]
pub struct BorrowRange<'a, R: ?Sized, K: ?Sized>(pub &'a R, pub std::marker::PhantomData<K>);

impl<'a, R, K> RangeBounds<[u8]> for BorrowRange<'a, R, K>
where
  R: RangeBounds<K>,
  K: ?Sized + std::borrow::Borrow<[u8]>,
{
  fn start_bound(&self) -> Bound<&[u8]> {
    match self.0.start_bound() {
      Bound::Included(b) => Bound::Included(<K as std::borrow::Borrow<[u8]>>::borrow(b)),
      Bound::Excluded(b) => Bound::Excluded(<K as std::borrow::Borrow<[u8]>>::borrow(b)),
      Bound::Unbounded => Bound::Unbounded,
    }
  }

  fn end_bound(&self) -> Bound<&[u8]> {
    match self.0.end_bound() {
      Bound::Included(b) => Bound::Included(<K as std::borrow::Borrow<[u8]>>::borrow(b)),
      Bound::Excluded(b) => Bound::Excluded(<K as std::borrow::Borrow<[u8]>>::borrow(b)),
      Bound::Unbounded => Bound::Unbounded,
    }
  }
}
